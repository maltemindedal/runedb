package test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/command"
	"github.com/maltemindedal/runedb/internal/config"
	runedblogger "github.com/maltemindedal/runedb/internal/logger"
	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/rdb"
	"github.com/maltemindedal/runedb/internal/server"
	"github.com/maltemindedal/runedb/internal/storage"
)

func TestServerHandlesMasterReplicaHandshake(t *testing.T) {
	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10

	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()

	parser := protocol.NewParser(conn)
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "REPLCONF", "listening-port", "6380")

	if err := protocol.WriteValue(conn, request("PSYNC", "?", "-1")); err != nil {
		t.Fatalf("WriteValue(PSYNC) error = %v", err)
	}

	fullResyncRaw, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() FULLRESYNC error = %v", err)
	}
	fullResync, ok := fullResyncRaw.(protocol.SimpleString)
	if !ok {
		t.Fatalf("FULLRESYNC type = %T, want protocol.SimpleString", fullResyncRaw)
	}
	parts := strings.Fields(fullResync.Value)
	if len(parts) != 3 || parts[0] != "FULLRESYNC" || parts[1] == "" || parts[2] != "0" {
		t.Fatalf("FULLRESYNC payload = %q, want 'FULLRESYNC <replid> 0'", fullResync.Value)
	}

	snapshotRaw, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() snapshot error = %v", err)
	}
	snapshot, ok := snapshotRaw.(protocol.BulkString)
	if !ok {
		t.Fatalf("snapshot type = %T, want protocol.BulkString", snapshotRaw)
	}
	if snapshot.Null {
		t.Fatal("snapshot bulk string unexpectedly null")
	}
	if string(snapshot.Data) != string(rdb.EmptySnapshot()) {
		t.Fatalf("snapshot payload = %q, want %q", string(snapshot.Data), string(rdb.EmptySnapshot()))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if srv.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", srv.ReplicaCount())
	}

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}
}

func TestServerReplicaModeInitiatesHandshake(t *testing.T) {
	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() fake master error = %v", err)
	}
	defer func() { _ = masterListener.Close() }()

	listeningPortCh := make(chan string, 1)
	handshakeDone := make(chan struct{})
	masterStop := make(chan struct{})
	masterErrCh := make(chan error, 1)

	go func() {
		conn, err := masterListener.Accept()
		if err != nil {
			masterErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		parser := protocol.NewParser(conn)
		writer := conn

		if err := assertReplicaRequest(parser, "PING"); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
			masterErrCh <- err
			return
		}

		replconf, err := decodeReplicaRequest(parser)
		if err != nil {
			masterErrCh <- err
			return
		}
		if replconf.Name != "REPLCONF" {
			masterErrCh <- fmt.Errorf("second request = %q, want REPLCONF", replconf.Name)
			return
		}
		if len(replconf.Args) != 2 || !strings.EqualFold(string(replconf.Args[0]), "listening-port") {
			masterErrCh <- fmt.Errorf("REPLCONF args = %q, want listening-port <port>", replconf.Args)
			return
		}
		listeningPortCh <- string(replconf.Args[1])
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "OK"}); err != nil {
			masterErrCh <- err
			return
		}

		if err := assertReplicaRequest(parser, "PSYNC", "?", "-1"); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.BulkString{Data: rdb.EmptySnapshot()}); err != nil {
			masterErrCh <- err
			return
		}

		close(handshakeDone)
		<-masterStop
		masterErrCh <- nil
	}()

	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.ReplicaOf = masterListener.Addr().String()

	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- srv.ListenAndServe(ctx)
	}()

	replicaAddr := waitForAddr(t, srv)
	_, replicaPortText, err := net.SplitHostPort(replicaAddr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", replicaAddr, err)
	}

	select {
	case announcedPort := <-listeningPortCh:
		if announcedPort != replicaPortText {
			t.Fatalf("REPLCONF listening-port = %q, want %q", announcedPort, replicaPortText)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not send REPLCONF in time")
	}

	select {
	case <-handshakeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not complete handshake in time")
	}

	clientConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
	}
	defer func() { _ = clientConn.Close() }()
	clientParser := protocol.NewParser(clientConn)
	assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "PONG"}, "PING")

	close(masterStop)

	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("fake master error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fake master did not stop within timeout")
	}

	cancel()
	select {
	case err := <-serverErrCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica server did not stop within timeout")
	}
}

func TestServerReplicaFullResyncReplacesExistingData(t *testing.T) {
	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() fake master error = %v", err)
	}
	defer func() { _ = masterListener.Close() }()

	handshakeDone := make(chan struct{})
	masterStop := make(chan struct{})
	masterErrCh := make(chan error, 1)
	snapshot := buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("fresh"), []byte("from-master")),
	)

	go func() {
		conn, err := masterListener.Accept()
		if err != nil {
			masterErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		parser := protocol.NewParser(conn)
		writer := conn

		if err := assertReplicaRequest(parser, "PING"); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
			masterErrCh <- err
			return
		}

		replconf, err := decodeReplicaRequest(parser)
		if err != nil {
			masterErrCh <- err
			return
		}
		if replconf.Name != "REPLCONF" || len(replconf.Args) != 2 || !strings.EqualFold(string(replconf.Args[0]), "listening-port") {
			masterErrCh <- fmt.Errorf("REPLCONF args = %q, want listening-port <port>", replconf.Args)
			return
		}
		if string(replconf.Args[1]) == "0" {
			masterErrCh <- fmt.Errorf("REPLCONF listening-port = %q, want an ephemeral non-zero port", replconf.Args[1])
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "OK"}); err != nil {
			masterErrCh <- err
			return
		}

		if err := assertReplicaRequest(parser, "PSYNC", "?", "-1"); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.BulkString{Data: snapshot}); err != nil {
			masterErrCh <- err
			return
		}

		close(handshakeDone)
		<-masterStop
		masterErrCh <- nil
	}()

	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("stale"), []byte("local")),
	))

	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.RDBPath = rdbPath
	cfg.ReplicaOf = masterListener.Addr().String()

	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- srv.ListenAndServe(ctx)
	}()

	replicaAddr := waitForAddr(t, srv)

	select {
	case <-handshakeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not complete FULLRESYNC in time")
	}

	clientConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
	}
	defer func() { _ = clientConn.Close() }()
	clientParser := protocol.NewParser(clientConn)

	assertCommandResponse(t, clientConn, clientParser, protocol.BulkString{Null: true}, "GET", "stale")
	assertCommandResponse(t, clientConn, clientParser, protocol.BulkString{Data: []byte("from-master")}, "GET", "fresh")

	close(masterStop)

	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("fake master error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fake master did not stop within timeout")
	}

	cancel()
	select {
	case err := <-serverErrCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica server did not stop within timeout")
	}
}

func assertReplicaRequest(parser *protocol.Parser, wantName string, wantArgs ...string) error {
	request, err := decodeReplicaRequest(parser)
	if err != nil {
		return err
	}
	if request.Name != wantName {
		return fmt.Errorf("request name = %q, want %q", request.Name, wantName)
	}
	if len(request.Args) != len(wantArgs) {
		return fmt.Errorf("request args len = %d, want %d", len(request.Args), len(wantArgs))
	}
	for i := range wantArgs {
		if string(request.Args[i]) != wantArgs[i] {
			return fmt.Errorf("request arg[%d] = %q, want %q", i, string(request.Args[i]), wantArgs[i])
		}
	}
	return nil
}

func decodeReplicaRequest(parser *protocol.Parser) (*command.Request, error) {
	value, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	return command.DecodeRequest(value)
}

func TestMasterPropagatesMutationsToReplica(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10

	logger := runedblogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	replicaAddr := waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	masterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
	}
	defer func() { _ = masterConn.Close() }()
	masterParser := protocol.NewParser(masterConn)

	replicaConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) replica error = %v", replicaAddr, err)
	}
	defer func() { _ = replicaConn.Close() }()
	replicaParser := protocol.NewParser(replicaConn)

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "name", "RuneDB")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("RuneDB")}, 2*time.Second, "GET", "name")

	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "DEL", "name")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Null: true}, 2*time.Second, "GET", "name")

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-key", "1")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "DEL", "missing")
	assertCommandResponse(t, masterConn, masterParser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.Integer{Value: 0},
	}}, "EXEC")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("1")}, 2*time.Second, "GET", "tx-key")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

func TestWaitReturnsReplicaAcknowledgements(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10

	logger := runedblogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	_ = waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	masterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
	}
	defer func() { _ = masterConn.Close() }()
	masterParser := protocol.NewParser(masterConn)

	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "wait-key", "ready")
	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

func TestWaitTimesOutWithoutReplicas(t *testing.T) {
	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10

	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "solo", "1")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "WAIT", "1", "50")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}
}

func TestWaitUsesPerClientReplicationOffset(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10

	logger := runedblogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	_ = waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	writerConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) writer error = %v", masterAddr, err)
	}
	defer func() { _ = writerConn.Close() }()
	writerParser := protocol.NewParser(writerConn)

	waiterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) waiter error = %v", masterAddr, err)
	}
	defer func() { _ = waiterConn.Close() }()
	waiterParser := protocol.NewParser(waiterConn)

	assertCommandResponse(t, writerConn, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "shared", "value")
	assertCommandResponse(t, waiterConn, waiterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

func assertEventuallyCommandResponse(t *testing.T, conn net.Conn, parser *protocol.Parser, want protocol.Value, timeout time.Duration, parts ...string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last protocol.Value
	var lastErr error

	for time.Now().Before(deadline) {
		if err := protocol.WriteValue(conn, request(parts...)); err != nil {
			t.Fatalf("WriteValue(%v) error = %v", parts, err)
		}

		got, err := parser.Parse()
		if err == nil && valuesEquivalent(got, want) {
			return
		}

		last = got
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("eventual Parse(%v) last error = %v", parts, lastErr)
	}
	t.Fatalf("eventual response for %v = %#v, want %#v", parts, last, want)
}

func valuesEquivalent(got protocol.Value, want protocol.Value) bool {
	gotEncoded, gotErr := protocol.Encode(got)
	wantEncoded, wantErr := protocol.Encode(want)
	if gotErr != nil || wantErr != nil {
		return false
	}

	return bytes.Equal(gotEncoded, wantEncoded)
}

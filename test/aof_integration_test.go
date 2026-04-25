package test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/command"
	"github.com/maltemindedal/runedb/internal/config"
	runedblogger "github.com/maltemindedal/runedb/internal/logger"
	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/server"
	"github.com/maltemindedal/runedb/internal/storage"
)

func TestServerPersistsAndReplaysAOF(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)

	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "RuneDB")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "expiring", "soon", "PX", "60000")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "HSET", "profile", "lang", "go")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "RPUSH", "letters", "a", "b")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "SADD", "tags", "fast", "durable")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "ZADD", "leaders", "1", "alpha", "2", "beta")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("1-0")}, "XADD", "events", "1-0", "type", "start")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-bad", "hello")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "INCR", "tx-bad")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-good", "1")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.ErrorValue{Message: "ERR value is not an integer or out of range"},
		protocol.SimpleString{Value: "OK"},
	}}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR unknown command \"NOPE\""}, "NOPE")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-abort", "1")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "EXECABORT Transaction discarded because of previous errors."}, "EXEC")

	watcherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) watcher error = %v", addr, err)
	}
	defer func() { _ = watcherConn.Close() }()
	watcherParser := protocol.NewParser(watcherConn)

	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "tx-watch")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "tx-watch", "1")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-watch", "2")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.Array{Null: true}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{}}, "EXEC")

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stop()
	waitForServerStop(t, errCh)

	restartCfg := testAOFConfig(aofPath)
	restartAddr, restartStop, restartErrCh := startTestServer(t, restartCfg)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer func() { _ = restartConn.Close() }()
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("RuneDB")}, "GET", "name")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("soon")}, "GET", "expiring")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("go")}, "HGET", "profile", "lang")
	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("a")},
		protocol.BulkString{Data: []byte("b")},
	}}, "LRANGE", "letters", "0", "-1")
	assertCommandResponse(t, restartConn, restartParser, protocol.Integer{Value: 1}, "SISMEMBER", "tags", "durable")
	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("alpha")},
		protocol.BulkString{Data: []byte("beta")},
	}}, "ZRANGE", "leaders", "0", "-1")
	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("events")},
			protocol.Array{Elements: []protocol.Value{
				protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("1-0")},
					protocol.Array{Elements: []protocol.Value{
						protocol.BulkString{Data: []byte("type")},
						protocol.BulkString{Data: []byte("start")},
					}},
				}},
			}},
		}},
	}}, "XREAD", "STREAMS", "events", "0-0")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("2")}, "GET", "counter")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("hello")}, "GET", "tx-bad")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "tx-good")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Null: true}, "GET", "tx-abort")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "tx-watch")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func TestServerPrefersAOFOverRDB(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "appendonly.aof")
	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("name"), []byte("rdb")),
	))
	payload, err := protocol.Encode(request("SET", "name", "aof"))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if err := os.WriteFile(aofPath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", aofPath, err)
	}

	cfg := testAOFConfig(aofPath)
	cfg.RDBPath = rdbPath
	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("aof")}, "GET", "name")

	stop()
	waitForServerStop(t, errCh)
}

func TestServerDoesNotPersistPublishToAOF(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	addr, stop, errCh := startTestServer(t, cfg)

	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer func() { _ = subscriberConn.Close() }()
	subscriberParser := protocol.NewParser(subscriberConn)
	if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "updates")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	if _, err := subscriberParser.Parse(); err != nil {
		t.Fatalf("Parse() SUBSCRIBE error = %v", err)
	}

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer func() { _ = publisherConn.Close() }()
	publisherParser := protocol.NewParser(publisherConn)
	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 1}, "PUBLISH", "updates", "hello")
	if _, err := subscriberParser.Parse(); err != nil {
		t.Fatalf("Parse() pushed pubsub message error = %v", err)
	}

	info, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", aofPath, err)
	}
	if info.Size() != 0 {
		t.Fatalf("AOF size after PUBLISH = %d, want 0", info.Size())
	}

	stop()
	waitForServerStop(t, errCh)
}

func TestServerRejectsInvalidReplicaConfigBeforeOpeningAOF(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	cfg.ReplicaOf = "not-a-host-port"

	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	err := srv.ListenAndServe(context.Background())
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want invalid replica configuration failure")
	}
	if _, statErr := os.Stat(aofPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want file not created", aofPath, statErr)
	}
}

func TestServerBGRewriteAOFCompactsAndReloads(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)

	for i := 0; i < 20; i++ {
		assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "value"+time.Date(2000, 1, 1, 0, 0, i, 0, time.UTC).Format("05"))
	}
	before, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Stat(%q) before rewrite error = %v", aofPath, err)
	}
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "Background append only file rewriting started"}, "BGREWRITEAOF")

	deadline := time.Now().Add(3 * time.Second)
	compacted := false
	for time.Now().Before(deadline) {
		info, statErr := os.Stat(aofPath)
		if statErr != nil {
			t.Fatalf("Stat(%q) during rewrite error = %v", aofPath, statErr)
		}
		if info.Size() < before.Size() {
			compacted = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !compacted {
		t.Fatalf("BGREWRITEAOF did not compact file within timeout (before=%d)", before.Size())
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stop()
	waitForServerStop(t, errCh)

	restartCfg := testAOFConfig(aofPath)
	restartAddr, restartStop, restartErrCh := startTestServer(t, restartCfg)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer func() { _ = restartConn.Close() }()
	restartParser := protocol.NewParser(restartConn)
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("value19")}, "GET", "name")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func defaultTestConfig() config.Config {
	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.DumpPath = ""
	return cfg
}

func testAOFConfig(aofPath string) config.Config {
	cfg := defaultTestConfig()
	cfg.AOFPath = aofPath
	cfg.AppendFsync = "always"
	return cfg
}

func startTestServer(t *testing.T, cfg config.Config) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	return waitForAddr(t, srv), cancel, errCh
}

func waitForServerStop(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop within timeout")
	}
}

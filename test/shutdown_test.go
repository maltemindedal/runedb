package test

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/protocol"
)

func TestServerShutdownClosesMultipleClients(t *testing.T) {
	cfg := defaultTestConfig()
	addr, cancel, errCh := startTestServer(t, cfg)
	clients := make([]net.Conn, 0, 5)
	parsers := make([]*protocol.Parser, 0, 5)
	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			cancel()
			t.Fatalf("Dial(%q) client %d error = %v", addr, i, err)
		}
		clients = append(clients, conn)
		parsers = append(parsers, protocol.NewParser(conn))
	}
	defer func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	}()

	for i := range clients {
		assertCommandResponse(t, clients[i], parsers[i], protocol.SimpleString{Value: "PONG"}, "PING")
	}

	cancel()
	waitForServerStop(t, errCh)

	for i, conn := range clients {
		if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline client %d error = %v", i, err)
		}

		buffer := make([]byte, 1)
		_, err := conn.Read(buffer)
		if err == nil {
			t.Fatalf("client %d read error = nil, want closed-connection error", i)
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatalf("client %d read timed out, want closed-connection error", i)
		}
	}
}

func TestServerShutdownUnblocksBLPop(t *testing.T) {
	cfg := defaultTestConfig()
	addr, cancel, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()

	if err := protocol.WriteValue(conn, request("BLPOP", "jobs")); err != nil {
		cancel()
		t.Fatalf("WriteValue(BLPOP) error = %v", err)
	}

	cancel()
	waitForServerStop(t, errCh)

	if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	buffer := make([]byte, 1)
	_, err = conn.Read(buffer)
	if err == nil {
		t.Fatal("conn.Read() error = nil, want closed-connection error")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("conn.Read() timed out, want closed-connection error")
	}
}

func TestServerShutdownPersistsSupportedSnapshotToRDB(t *testing.T) {
	dumpPath := filepath.Join(t.TempDir(), "dump.rdb")

	cfg := defaultTestConfig()
	cfg.DumpPath = dumpPath

	addr, cancel, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "RuneDB")

	cancel()
	waitForServerStop(t, errCh)

	restartCfg := defaultTestConfig()
	restartCfg.RDBPath = dumpPath
	restartCfg.DumpPath = dumpPath

	restartAddr, restartCancel, restartErrCh := startTestServer(t, restartCfg)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		restartCancel()
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer func() { _ = restartConn.Close() }()
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("RuneDB")}, "GET", "name")

	restartCancel()
	waitForServerStop(t, restartErrCh)
}

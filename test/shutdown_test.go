package test

import (
	"context"
	"net"
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

func TestServerShutdownClosesMultipleClients(t *testing.T) {
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
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
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
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}

	for i, conn := range clients {
		if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline client %d error = %v", i, err)
		}

		buffer := make([]byte, 1)
		_, err := conn.Read(buffer)
		if err == nil {
			t.Fatalf("client %d read error = nil, want closed-connection error", i)
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatalf("client %d read timed out, want closed-connection error", i)
		}
	}
}

func TestServerShutdownUnblocksBLPop(t *testing.T) {
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
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
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
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}

	if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	buffer := make([]byte, 1)
	_, err = conn.Read(buffer)
	if err == nil {
		t.Fatal("conn.Read() error = nil, want closed-connection error")
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("conn.Read() timed out, want closed-connection error")
	}
}

func TestServerShutdownPersistsSupportedSnapshotToRDB(t *testing.T) {
	dumpPath := filepath.Join(t.TempDir(), "dump.rdb")

	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.DumpPath = dumpPath

	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "RuneDB")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}

	restartCfg := config.Default()
	restartCfg.Host = "127.0.0.1"
	restartCfg.Port = 0
	restartCfg.LogLevel = "error"
	restartCfg.EvictionInterval = 5 * time.Millisecond
	restartCfg.EvictionSampleSize = 10
	restartCfg.RDBPath = dumpPath

	restartLogger := runedblogger.New(restartCfg.LogLevel)
	restartStore := storage.NewStore()
	restartExecutor := command.NewExecutor(restartStore, restartLogger)
	restartServer := server.New(restartCfg, restartLogger, restartStore, restartExecutor)

	restartCtx, restartCancel := context.WithCancel(context.Background())
	restartErrCh := make(chan error, 1)
	go func() {
		restartErrCh <- restartServer.ListenAndServe(restartCtx)
	}()

	restartAddr := waitForAddr(t, restartServer)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		restartCancel()
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer func() { _ = restartConn.Close() }()
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("RuneDB")}, "GET", "name")

	restartCancel()
	select {
	case err := <-restartErrCh:
		if err != nil {
			t.Fatalf("restart ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart server did not stop within timeout")
	}
}

package test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maltemindedal/godis/internal/command"
	"github.com/maltemindedal/godis/internal/config"
	godislogger "github.com/maltemindedal/godis/internal/logger"
	"github.com/maltemindedal/godis/internal/protocol"
	"github.com/maltemindedal/godis/internal/server"
	"github.com/maltemindedal/godis/internal/storage"
)

func TestServerShutdownClosesMultipleClients(t *testing.T) {
	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10

	logger := godislogger.New(cfg.LogLevel)
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

	logger := godislogger.New(cfg.LogLevel)
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

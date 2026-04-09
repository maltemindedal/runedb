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

func TestServerHandlesPhaseOneCommands(t *testing.T) {
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
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("failed to close test connection: %v", closeErr)
		}
	}()

	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("hello")}, "ECHO", "hello")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "godis")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("godis")}, "GET", "name")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "DEL", "name")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "name")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "bad", "hello")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR value is not an integer or out of range"}, "INCR", "bad")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "temp", "1", "PX", "15")

	time.Sleep(25 * time.Millisecond)
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "temp")

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

func waitForAddr(t *testing.T, srv *server.Server) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("SplitHostPort(%q) error = %v", addr, err)
			}
			return net.JoinHostPort("127.0.0.1", port)
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("server address not available before timeout")
	return ""
}

func assertCommandResponse(t *testing.T, conn net.Conn, parser *protocol.Parser, want protocol.Value, parts ...string) {
	t.Helper()

	if err := protocol.WriteValue(conn, request(parts...)); err != nil {
		t.Fatalf("WriteValue(%v) error = %v", parts, err)
	}

	got, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	assertValuesEqual(t, got, want)
}

func request(parts ...string) protocol.Value {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	return protocol.Array{Elements: elements}
}

func assertValuesEqual(t *testing.T, got protocol.Value, want protocol.Value) {
	t.Helper()

	switch typedWant := want.(type) {
	case protocol.SimpleString:
		typedGot, ok := got.(protocol.SimpleString)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("simple string = %q, want %q", typedGot.Value, typedWant.Value)
		}
	case protocol.BulkString:
		typedGot, ok := got.(protocol.BulkString)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null {
			t.Fatalf("bulk null = %v, want %v", typedGot.Null, typedWant.Null)
		}
		if string(typedGot.Data) != string(typedWant.Data) {
			t.Fatalf("bulk string = %q, want %q", string(typedGot.Data), string(typedWant.Data))
		}
	case protocol.Integer:
		typedGot, ok := got.(protocol.Integer)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("integer = %d, want %d", typedGot.Value, typedWant.Value)
		}
	case protocol.ErrorValue:
		typedGot, ok := got.(protocol.ErrorValue)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Message != typedWant.Message {
			t.Fatalf("error message = %q, want %q", typedGot.Message, typedWant.Message)
		}
	default:
		t.Fatalf("unsupported wanted type %T", want)
	}
}

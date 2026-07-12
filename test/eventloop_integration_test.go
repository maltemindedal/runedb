//go:build linux || darwin

package test

import (
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/config"
	"github.com/maltemindedal/runedb/internal/protocol"
)

func eventLoopTestConfig() config.Config {
	cfg := defaultTestConfig()
	cfg.EventLoop = true
	return cfg
}

func TestEventLoopServesRepresentativeCommands(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("hello")}, "ECHO", "hello")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "greeting", "world")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("world")}, "GET", "greeting")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "missing")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "INCR", "counter")

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-key", "tx-value")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.Integer{Value: 2},
	}}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("tx-value")}, "GET", "tx-key")

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopServesPipelinedRequests(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	// Send a pipeline of frames in one burst, including a trailing frame split
	// across two writes, and expect ordered replies.
	pipeline := "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfo"
	if _, err := conn.Write([]byte(pipeline)); err != nil {
		t.Fatalf("Write(pipeline) error = %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := conn.Write([]byte("o\r\n")); err != nil {
		t.Fatalf("Write(pipeline tail) error = %v", err)
	}

	assertParsedValue(t, parser, protocol.SimpleString{Value: "PONG"})
	assertParsedValue(t, parser, protocol.SimpleString{Value: "OK"})
	assertParsedValue(t, parser, protocol.BulkString{Data: []byte("bar")})

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopDeliversPubSubMessages(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberConn)
	subscriberParser := protocol.NewParser(subscriberConn)

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "updates")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}})

	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 1}, "PUBLISH", "updates", "hello")
	assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

	if err := protocol.WriteValue(subscriberConn, request("UNSUBSCRIBE")); err != nil {
		t.Fatalf("WriteValue(UNSUBSCRIBE) error = %v", err)
	}
	assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "unsubscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 0},
	}})
	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 0}, "PUBLISH", "updates", "bye")

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopClosesConnectionAfterProtocolError(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	if _, err := conn.Write([]byte("!!!bogus\r\n")); err != nil {
		t.Fatalf("Write(bogus frame) error = %v", err)
	}

	reply, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() protocol error reply error = %v", err)
	}
	if _, ok := reply.(protocol.ErrorValue); !ok {
		t.Fatalf("reply = %#v, want protocol.ErrorValue", reply)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if _, err := parser.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("Parse() after protocol error = %v, want io.EOF", err)
	}

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopServesManyIdleConnectionsWithoutPerConnectionGoroutines(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	const idleConns = 200

	baseline := runtime.NumGoroutine()

	conns := make([]net.Conn, 0, idleConns)
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for i := 0; i < idleConns; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) idle connection %d error = %v", addr, i, err)
		}
		conns = append(conns, conn)
	}

	// Prove every connection is accepted and served, not just queued.
	first := protocol.NewParser(conns[0])
	assertCommandResponse(t, conns[0], first, protocol.SimpleString{Value: "PONG"}, "PING")
	last := protocol.NewParser(conns[idleConns-1])
	assertCommandResponse(t, conns[idleConns-1], last, protocol.SimpleString{Value: "PONG"}, "PING")

	grown := runtime.NumGoroutine() - baseline
	if grown >= idleConns/2 {
		t.Fatalf("goroutine count grew by %d for %d idle connections, want event-loop scaling far below one goroutine per connection", grown, idleConns)
	}

	stop()
	waitForServerStop(t, errCh)
}

func assertParsedValue(t *testing.T, parser *protocol.Parser, want protocol.Value) {
	t.Helper()

	got, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	assertValuesEqual(t, got, want)
}

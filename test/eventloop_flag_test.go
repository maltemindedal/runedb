package test

import (
	"net"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// TestEventLoopFlagServesClientsOnEveryPlatform verifies the event-loop
// configuration serves clients on every platform: through OS I/O multiplexing
// where supported (Linux, macOS) and through the goroutine-per-connection
// fallback elsewhere.
func TestEventLoopFlagServesClientsOnEveryPlatform(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.EventLoop = true

	addr, stop, errCh := startTestServer(t, cfg)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "mode", "event-loop")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("event-loop")}, "GET", "mode")

	stop()
	waitForServerStop(t, errCh)
}

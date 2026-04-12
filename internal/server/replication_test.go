package server

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/config"
	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/storage"
)

func TestReplicaRegistryCountReplicasAtOrAboveWithNotify(t *testing.T) {
	registry := NewReplicaRegistry()
	serverConn, replicaConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = replicaConn.Close() }()

	registry.Add(1, serverConn, 6380)

	count, changed := registry.CountReplicasAtOrAboveWithNotify(10)
	if count != 0 {
		t.Fatalf("initial count = %d, want 0", count)
	}

	if ok := registry.UpdateAck(1, 10); !ok {
		t.Fatal("UpdateAck() = false, want true")
	}

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("CountReplicasAtOrAboveWithNotify() channel was not notified")
	}

	count, _ = registry.CountReplicasAtOrAboveWithNotify(10)
	if count != 1 {
		t.Fatalf("updated count = %d, want 1", count)
	}
}

func TestReplicaRegistryRemoveAndCloseReturnsCloseError(t *testing.T) {
	registry := NewReplicaRegistry()
	registry.Add(1, &stubConn{closeErr: errors.New("close boom")}, 6380)

	err := registry.RemoveAndClose(1)
	if err == nil || err.Error() != "close boom" {
		t.Fatalf("RemoveAndClose() error = %v, want close boom", err)
	}
}

func TestServerPropagateToReplicasRemovesFailingReplica(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(config.Config{}, logger, storage.NewStore(), nil)

	serverConn := &recordingConn{}
	srv.replicaPeers.Add(1, serverConn, 6380)
	srv.replicaPeers.Add(2, &stubConn{writeErr: errors.New("write boom")}, 6381)

	report := srv.propagateToReplicas([]protocol.Value{protocol.SimpleString{Value: "OK"}})
	if report.attempted != 2 {
		t.Fatalf("report.attempted = %d, want 2", report.attempted)
	}
	if report.succeeded != 1 {
		t.Fatalf("report.succeeded = %d, want 1", report.succeeded)
	}
	if report.failed != 1 {
		t.Fatalf("report.failed = %d, want 1", report.failed)
	}
	if srv.replicaPeers.Count() != 1 {
		t.Fatalf("replicaPeers.Count() = %d, want 1", srv.replicaPeers.Count())
	}

	parser := protocol.NewParser(bytes.NewReader(serverConn.Bytes()))
	value, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got, ok := value.(protocol.SimpleString); !ok || got.Value != "OK" {
		t.Fatalf("parsed value = %#v, want simple string OK", value)
	}
}

type stubConn struct {
	writeErr error
	closeErr error
}

type recordingConn struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *stubConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *stubConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}
func (c *stubConn) Close() error                       { return c.closeErr }
func (c *stubConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *stubConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *stubConn) SetDeadline(_ time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}
func (c *recordingConn) Close() error                       { return nil }
func (c *recordingConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *recordingConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *recordingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buf.Bytes())
}

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/maltemindedal/runedb/internal/protocol"
)

func echoRunner(t *testing.T) (ConnCommandRunner, *[]protocol.Value) {
	t.Helper()

	executed := &[]protocol.Value{}
	run := func(_ context.Context, request protocol.Value) ([]protocol.Value, error) {
		*executed = append(*executed, request)
		return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
	}
	return run, executed
}

func flushAll(t *testing.T, machine *ConnMachine) []byte {
	t.Helper()

	var out bytes.Buffer
	for machine.HasPendingOutput() {
		if err := machine.Flush(&out); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}
	return out.Bytes()
}

// shortWriter accepts at most limit bytes per Write call to model a transport
// that applies backpressure.
type shortWriter struct {
	buf   bytes.Buffer
	limit int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return w.buf.Write(p)
}

func TestConnMachinePartialReads(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(1, nil)

	request := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	for i, b := range request {
		if err := machine.Feed([]byte{b}); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		if i < len(request)-1 && machine.PendingRequests() != 0 {
			t.Fatalf("PendingRequests() = %d after %d bytes, want 0", machine.PendingRequests(), i+1)
		}
	}

	if machine.PendingRequests() != 1 {
		t.Fatalf("PendingRequests() = %d, want 1", machine.PendingRequests())
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if len(*executed) != 1 {
		t.Fatalf("executed %d requests, want 1", len(*executed))
	}

	if got, want := flushAll(t, machine), "+OK\r\n"; string(got) != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
	if machine.State() != ConnStateActive {
		t.Fatalf("State() = %d, want ConnStateActive", machine.State())
	}
}

func TestConnMachineCommandBoundaries(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(1, nil)

	// Two complete requests plus the beginning of a third in one readable chunk.
	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$3\r\nhe")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.PendingRequests() != 2 {
		t.Fatalf("PendingRequests() = %d, want 2", machine.PendingRequests())
	}

	if err := machine.Feed([]byte("y\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.PendingRequests() != 3 {
		t.Fatalf("PendingRequests() = %d, want 3", machine.PendingRequests())
	}

	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if len(*executed) != 3 {
		t.Fatalf("executed %d requests, want 3", len(*executed))
	}

	third, ok := (*executed)[2].(protocol.Array)
	if !ok || len(third.Elements) != 2 {
		t.Fatalf("third request = %#v, want two-element array", (*executed)[2])
	}
	payload, ok := third.Elements[1].(protocol.BulkString)
	if !ok || string(payload.Data) != "hey" {
		t.Fatalf("third request payload = %#v, want %q", third.Elements[1], "hey")
	}

	if got, want := flushAll(t, machine), "+OK\r\n+OK\r\n+OK\r\n"; string(got) != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
}

func TestConnMachinePartialWrites(t *testing.T) {
	machine := NewConnMachine(1, nil)
	run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
		return []protocol.Value{protocol.BulkString{Data: []byte("a longer payload that needs several flushes")}}, nil
	}

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}

	writer := &shortWriter{limit: 7}
	flushes := 0
	for machine.HasPendingOutput() {
		if err := machine.Flush(writer); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		flushes++
	}

	want := "$43\r\na longer payload that needs several flushes\r\n"
	if got := writer.buf.String(); got != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
	if flushes < 2 {
		t.Fatalf("flushes = %d, want multiple partial writes", flushes)
	}
	if machine.State() != ConnStateActive {
		t.Fatalf("State() = %d, want ConnStateActive", machine.State())
	}
}

func TestConnMachineProtocolErrorClosesAfterOrderedReplies(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(1, nil)

	// One valid request followed by an unsupported frame prefix.
	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\nX\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.State() != ConnStateClosing {
		t.Fatalf("State() = %d, want ConnStateClosing", machine.State())
	}
	if machine.Err() == nil {
		t.Fatal("Err() = nil, want recorded protocol error")
	}

	if err := machine.Feed([]byte("+more\r\n")); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("Feed() after protocol error = %v, want ErrConnMachineClosed", err)
	}

	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if len(*executed) != 1 {
		t.Fatalf("executed %d requests, want 1", len(*executed))
	}

	var out bytes.Buffer
	for machine.State() != ConnStateClosed {
		if err := machine.Flush(&out); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}

	reader := protocol.NewParser(bufio.NewReader(bytes.NewReader(out.Bytes())))
	first, err := reader.Parse()
	if err != nil {
		t.Fatalf("parse first reply: %v", err)
	}
	if want := (protocol.SimpleString{Value: "OK"}); first != want {
		t.Fatalf("first reply = %#v, want %#v", first, want)
	}
	second, err := reader.Parse()
	if err != nil {
		t.Fatalf("parse second reply: %v", err)
	}
	if _, ok := second.(protocol.ErrorValue); !ok {
		t.Fatalf("second reply = %#v, want protocol error reply", second)
	}
	if _, err := reader.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("trailing parse error = %v, want io.EOF", err)
	}
}

func TestConnMachineRunnerErrorClosesImmediately(t *testing.T) {
	machine := NewConnMachine(1, nil)
	fatal := fmt.Errorf("execution pipeline failed")
	run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
		return nil, fatal
	}

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); !errors.Is(err, fatal) {
		t.Fatalf("ProcessPending() error = %v, want %v", err, fatal)
	}

	if machine.State() != ConnStateClosed {
		t.Fatalf("State() = %d, want ConnStateClosed", machine.State())
	}
	if !errors.Is(machine.Err(), fatal) {
		t.Fatalf("Err() = %v, want %v", machine.Err(), fatal)
	}
	if machine.HasPendingOutput() {
		t.Fatal("HasPendingOutput() = true, want discarded output after fatal close")
	}
}

func TestConnMachineCloseCleansUpClientState(t *testing.T) {
	state := &ClientState{ID: 7}
	state.BindResponseWriter(bufio.NewWriter(io.Discard))
	if !state.BeginTransaction() {
		t.Fatal("BeginTransaction() = false, want true")
	}
	state.EnqueueCommand("SET", [][]byte{[]byte("foo"), []byte("bar")})

	machine := NewConnMachine(7, state)
	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}

	machine.Close(nil)

	if machine.State() != ConnStateClosed {
		t.Fatalf("State() = %d, want ConnStateClosed", machine.State())
	}
	if state.InTransactionActive() {
		t.Fatal("InTransactionActive() = true, want transaction reset on close")
	}
	if state.HasActiveResponseWriter() {
		t.Fatal("HasActiveResponseWriter() = true, want response writer detached on close")
	}

	if err := machine.Feed([]byte("+ping\r\n")); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("Feed() after close = %v, want ErrConnMachineClosed", err)
	}
	run, _ := echoRunner(t)
	if err := machine.ProcessPending(context.Background(), run); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("ProcessPending() after close = %v, want ErrConnMachineClosed", err)
	}
	if err := machine.Flush(io.Discard); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("Flush() after close = %v, want ErrConnMachineClosed", err)
	}
}

func TestConnMachineCloseIsIdempotent(t *testing.T) {
	machine := NewConnMachine(1, nil)
	first := fmt.Errorf("peer reset")

	machine.Close(first)
	machine.Close(fmt.Errorf("later error"))

	if !errors.Is(machine.Err(), first) {
		t.Fatalf("Err() = %v, want first close error %v", machine.Err(), first)
	}
}

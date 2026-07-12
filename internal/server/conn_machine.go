package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/maltemindedal/runedb/internal/protocol"
)

// ConnMachineState identifies the lifecycle phase of a connection state machine.
type ConnMachineState int

const (
	// ConnStateActive accepts readable input, executes parsed requests, and
	// buffers responses.
	ConnStateActive ConnMachineState = iota
	// ConnStateClosing rejects new input while draining already-parsed
	// requests and pending output before closing.
	ConnStateClosing
	// ConnStateClosed has released connection resources; all operations fail.
	ConnStateClosed
)

// defaultMaxReadBuffer bounds the bytes a single incomplete frame may buffer
// before the machine rejects it as a protocol error, preventing a client from
// exhausting memory with an oversized or never-terminated frame. It mirrors the
// role of Redis's proto-max-bulk-len query-buffer limit.
const defaultMaxReadBuffer = 512 * 1024 * 1024

// ConnCommandRunner executes one parsed request with connection-scoped state
// and returns the RESP responses to buffer for the client. A non-nil error is
// fatal for the connection.
type ConnCommandRunner func(ctx context.Context, request protocol.Value) ([]protocol.Value, error)

// ErrConnMachineClosed reports an operation on a connection state machine that
// no longer accepts it.
var ErrConnMachineClosed = errors.New("server: connection state machine closed")

// connEvent is one ordered unit of work parsed from readable input: either a
// complete request or a permanent protocol error whose RESP error reply must
// be emitted after the responses of earlier requests.
type connEvent struct {
	request  protocol.Value
	protoErr error
}

// ConnMachine is an explicit non-blocking connection state machine. It buffers
// readable bytes, parses complete RESP requests, executes them through a
// command runner, buffers RESP responses, and flushes pending output
// incrementally, without assuming a dedicated blocking goroutine per client.
//
// Authentication, subscription, transaction, and monitor state live on the
// bound ClientState, which the machine detaches from shared registries when
// the connection closes.
//
// On a permanent protocol error the machine emits an ordered error reply and
// then closes the connection. This intentionally diverges from the
// goroutine-per-connection handler, which replies and keeps serving; closing
// matches Redis, which terminates a connection after a protocol error because
// the byte stream can no longer be resynchronized. The divergence is confined
// to event-loop mode, which drives connections through this machine, and does
// not affect the default networking path.
//
// A ConnMachine is not safe for concurrent use; a single driving loop must own
// it. Cross-goroutine async deliveries continue to flow through ClientState,
// which carries its own locking.
type ConnMachine struct {
	client *ClientState

	state         ConnMachineState
	err           error
	readBuf       []byte
	resumeAt      int
	maxReadBuffer int
	pending       []connEvent
	writeBuf      []byte
	writeOff      int
}

// NewConnMachine constructs an active connection state machine bound to the
// connection-scoped client state.
func NewConnMachine(client *ClientState) *ConnMachine {
	return &ConnMachine{
		client:        client,
		maxReadBuffer: defaultMaxReadBuffer,
	}
}

// State returns the current lifecycle phase.
func (m *ConnMachine) State() ConnMachineState {
	return m.state
}

// Err returns the error that moved the machine toward close, if any.
func (m *ConnMachine) Err() error {
	return m.err
}

// PendingRequests reports how many parsed requests await execution.
func (m *ConnMachine) PendingRequests() int {
	count := 0
	for _, event := range m.pending {
		if event.protoErr == nil {
			count++
		}
	}
	return count
}

// HasPendingOutput reports whether buffered response bytes await flushing.
func (m *ConnMachine) HasPendingOutput() bool {
	return m.writeOff < len(m.writeBuf)
}

// Feed appends readable bytes to the read buffer and parses every complete
// RESP request they finish. Incomplete trailing frames stay buffered until
// more bytes arrive; the machine defers re-decoding until the buffer reaches
// the byte count the pending frame is known to need, so a frame arriving in
// many small chunks is not rescanned on every append. A permanent protocol
// error — including a frame exceeding the read-buffer limit — queues an ordered
// RESP error reply and transitions the machine to the closing state, so callers
// should check State after feeding.
func (m *ConnMachine) Feed(data []byte) error {
	if m.state != ConnStateActive {
		return ErrConnMachineClosed
	}

	m.readBuf = append(m.readBuf, data...)
	if m.resumeAt > 0 && len(m.readBuf) < m.resumeAt {
		return nil
	}

	consumed := 0
	for {
		value, n, err := protocol.Decode(m.readBuf[consumed:])
		if errors.Is(err, protocol.ErrIncomplete) {
			m.resumeAt = 0
			var incomplete *protocol.IncompleteError
			if errors.As(err, &incomplete) {
				m.resumeAt = incomplete.Need
			}
			break
		}
		if err != nil {
			m.enterProtocolError(err)
			return nil
		}

		m.pending = append(m.pending, connEvent{request: value})
		consumed += n
	}

	m.readBuf = append(m.readBuf[:0], m.readBuf[consumed:]...)

	if len(m.readBuf) > m.maxReadBuffer || m.resumeAt > m.maxReadBuffer {
		m.enterProtocolError(fmt.Errorf("protocol: frame exceeds %d byte read-buffer limit", m.maxReadBuffer))
	}
	return nil
}

// enterProtocolError queues an ordered error reply, records the fatal cause,
// and transitions to the closing state so buffered replies still drain.
func (m *ConnMachine) enterProtocolError(err error) {
	m.pending = append(m.pending, connEvent{protoErr: err})
	m.err = err
	m.state = ConnStateClosing
	m.readBuf = nil
	m.resumeAt = 0
}

// ProcessPending executes parsed requests in arrival order through run and
// buffers their RESP responses. A queued protocol error emits its RESP error
// reply in order. A runner error is fatal and closes the machine immediately.
func (m *ConnMachine) ProcessPending(ctx context.Context, run ConnCommandRunner) error {
	if m.state == ConnStateClosed {
		return ErrConnMachineClosed
	}

	for len(m.pending) > 0 {
		event := m.pending[0]
		m.pending = m.pending[1:]

		if event.protoErr != nil {
			if err := m.bufferResponses([]protocol.Value{protocol.ErrorValue{Message: "ERR " + event.protoErr.Error()}}); err != nil {
				m.Close(err)
				return err
			}
			continue
		}

		responses, err := run(ctx, event.request)
		if err != nil {
			m.Close(err)
			return err
		}
		if err := m.bufferResponses(responses); err != nil {
			m.Close(err)
			return err
		}
	}

	return nil
}

// Flush writes pending output to w and consumes whatever w accepts, so partial
// writes leave the remainder buffered for a later call. The unwritten remainder
// is tracked by an offset rather than recompacted, so draining a large reply to
// a slow reader stays linear. Once a closing machine has drained its parsed
// requests and pending output, Flush completes the transition to the closed
// state.
func (m *ConnMachine) Flush(w io.Writer) error {
	if m.state == ConnStateClosed {
		return ErrConnMachineClosed
	}

	if m.writeOff < len(m.writeBuf) {
		n, err := w.Write(m.writeBuf[m.writeOff:])
		m.writeOff += n
		if m.writeOff >= len(m.writeBuf) {
			m.writeBuf = m.writeBuf[:0]
			m.writeOff = 0
		}
		if err != nil {
			return err
		}
	}

	if m.state == ConnStateClosing && !m.HasPendingOutput() && len(m.pending) == 0 {
		m.Close(nil)
	}

	return nil
}

// BufferEncoded appends a pre-encoded RESP payload to the pending output
// buffer. The event loop uses it to deliver asynchronous push frames (pub/sub
// messages, monitor events, replication payloads) produced by other
// connections, keeping every byte written to the socket ordered through the
// machine's single write buffer. A closing machine still accepts payloads so
// they drain with the remaining output; a closed machine rejects them.
func (m *ConnMachine) BufferEncoded(payload []byte) error {
	if m.state == ConnStateClosed {
		return ErrConnMachineClosed
	}
	if len(payload) == 0 {
		return nil
	}

	if m.writeOff > 0 {
		m.writeBuf = append(m.writeBuf[:0], m.writeBuf[m.writeOff:]...)
		m.writeOff = 0
	}

	m.writeBuf = append(m.writeBuf, payload...)
	return nil
}

// Close transitions to the closed state, discards buffered input and output,
// and detaches the connection-scoped client state from shared registries.
func (m *ConnMachine) Close(err error) {
	if m.state == ConnStateClosed {
		return
	}

	if m.err == nil {
		m.err = err
	}
	m.state = ConnStateClosed
	m.readBuf = nil
	m.resumeAt = 0
	m.pending = nil
	m.writeBuf = nil
	m.writeOff = 0
	m.client.Disconnect()
}

func (m *ConnMachine) bufferResponses(values []protocol.Value) error {
	if len(values) == 0 {
		return nil
	}

	if m.writeOff > 0 {
		m.writeBuf = append(m.writeBuf[:0], m.writeBuf[m.writeOff:]...)
		m.writeOff = 0
	}

	encoded, err := protocol.AppendValues(m.writeBuf, values)
	if err != nil {
		return err
	}

	m.writeBuf = encoded
	return nil
}

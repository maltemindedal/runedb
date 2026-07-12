package server

import (
	"context"
	"errors"
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
// A ConnMachine is not safe for concurrent use; a single driving loop must own
// it. Cross-goroutine async deliveries continue to flow through ClientState,
// which carries its own locking.
type ConnMachine struct {
	clientID uint64
	client   *ClientState

	state    ConnMachineState
	err      error
	readBuf  []byte
	pending  []connEvent
	writeBuf []byte
}

// NewConnMachine constructs an active connection state machine bound to the
// connection-scoped client state.
func NewConnMachine(clientID uint64, client *ClientState) *ConnMachine {
	return &ConnMachine{
		clientID: clientID,
		client:   client,
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

// ClientID returns the connection registry identifier.
func (m *ConnMachine) ClientID() uint64 {
	return m.clientID
}

// Client returns the bound connection-scoped client state.
func (m *ConnMachine) Client() *ClientState {
	return m.client
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
	return len(m.writeBuf) > 0
}

// Feed appends readable bytes to the read buffer and parses every complete
// RESP request they finish. Incomplete trailing frames stay buffered until
// more bytes arrive. A permanent protocol error queues an ordered RESP error
// reply and transitions the machine to the closing state, so callers should
// check State after feeding.
func (m *ConnMachine) Feed(data []byte) error {
	if m.state != ConnStateActive {
		return ErrConnMachineClosed
	}

	m.readBuf = append(m.readBuf, data...)
	consumed := 0
	for {
		value, n, err := protocol.Decode(m.readBuf[consumed:])
		if errors.Is(err, protocol.ErrIncomplete) {
			break
		}
		if err != nil {
			m.pending = append(m.pending, connEvent{protoErr: err})
			m.err = err
			m.state = ConnStateClosing
			m.readBuf = nil
			return nil
		}

		m.pending = append(m.pending, connEvent{request: value})
		consumed += n
	}

	m.readBuf = append(m.readBuf[:0], m.readBuf[consumed:]...)
	return nil
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
// writes leave the remainder buffered for a later call. Once a closing machine
// has drained its parsed requests and pending output, Flush completes the
// transition to the closed state.
func (m *ConnMachine) Flush(w io.Writer) error {
	if m.state == ConnStateClosed {
		return ErrConnMachineClosed
	}

	if len(m.writeBuf) > 0 {
		n, err := w.Write(m.writeBuf)
		m.writeBuf = append(m.writeBuf[:0], m.writeBuf[n:]...)
		if err != nil {
			return err
		}
	}

	if m.state == ConnStateClosing && len(m.writeBuf) == 0 && len(m.pending) == 0 {
		m.Close(nil)
	}

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
	m.pending = nil
	m.writeBuf = nil
	m.client.Disconnect()
}

func (m *ConnMachine) bufferResponses(values []protocol.Value) error {
	if len(values) == 0 {
		return nil
	}

	payload, err := protocol.EncodeValues(values)
	if err != nil {
		return err
	}

	m.writeBuf = append(m.writeBuf, payload...)
	return nil
}

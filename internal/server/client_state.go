package server

import (
	"context"
	"sync"
)

// QueuedCommand stores command metadata for future transaction support.
type QueuedCommand struct {
	Name string
	Args [][]byte
}

// ClientState holds connection-scoped state for auth and transaction features.
type ClientState struct {
	ID uint64

	mu sync.Mutex

	Authenticated bool
	InTransaction bool
	TxQueue       []QueuedCommand
}

type clientStateContextKey struct{}

// WithClientState attaches a connection-scoped client state to ctx.
func WithClientState(ctx context.Context, state *ClientState) context.Context {
	return context.WithValue(ctx, clientStateContextKey{}, state)
}

// ClientStateFromContext retrieves the connection-scoped client state from ctx.
func ClientStateFromContext(ctx context.Context) (*ClientState, bool) {
	state, ok := ctx.Value(clientStateContextKey{}).(*ClientState)
	return state, ok
}

// QueueLength returns the number of currently queued transaction commands.
func (s *ClientState) QueueLength() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.TxQueue)
}

// ResetTransaction clears queued transaction state.
func (s *ClientState) ResetTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.InTransaction = false
	s.TxQueue = nil
}

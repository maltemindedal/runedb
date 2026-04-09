package server

import (
	"bytes"
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

	mu sync.RWMutex

	watchRegistry *WatchRegistry
	watchedKeys   map[string]struct{}

	Authenticated bool
	InTransaction bool
	TxFailed      bool
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.TxQueue)
}

// BeginTransaction marks the client as inside a transaction.
// It returns false when the client is already inside a transaction.
func (s *ClientState) BeginTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.InTransaction {
		return false
	}

	s.InTransaction = true
	s.TxQueue = nil
	return true
}

// SetWatchRegistry binds the shared watch registry to the client state.
func (s *ClientState) SetWatchRegistry(registry *WatchRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchRegistry = registry
	if s.watchedKeys == nil {
		s.watchedKeys = make(map[string]struct{})
	}
}

// InTransactionActive reports whether the client is currently inside a transaction.
func (s *ClientState) InTransactionActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.InTransaction
}

// EnqueueCommand appends a command to the transaction queue.
func (s *ClientState) EnqueueCommand(name string, args [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queuedArgs := make([][]byte, 0, len(args))
	for _, arg := range args {
		queuedArgs = append(queuedArgs, bytes.Clone(arg))
	}

	s.TxQueue = append(s.TxQueue, QueuedCommand{
		Name: name,
		Args: queuedArgs,
	})
}

// WatchKeys registers the supplied keys for optimistic locking.
func (s *ClientState) WatchKeys(keys ...string) {
	s.mu.RLock()
	registry := s.watchRegistry
	s.mu.RUnlock()

	if registry == nil {
		return
	}

	registry.Watch(s, keys...)
}

// UnwatchAll removes all watched keys for the client and clears failure state.
func (s *ClientState) UnwatchAll() {
	s.mu.RLock()
	registry := s.watchRegistry
	s.mu.RUnlock()

	if registry != nil {
		registry.UnwatchAll(s)
	}

	s.ClearTransactionFailure()
}

// TransactionFailed reports whether optimistic locking marked the transaction as aborted.
func (s *ClientState) TransactionFailed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.TxFailed
}

// MarkTransactionFailed marks the transaction as invalidated by a watched-key mutation.
func (s *ClientState) MarkTransactionFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TxFailed = true
}

// ClearTransactionFailure resets the optimistic-locking failure flag.
func (s *ClientState) ClearTransactionFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TxFailed = false
}

func (s *ClientState) addWatchedKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.watchedKeys == nil {
		s.watchedKeys = make(map[string]struct{})
	}
	s.watchedKeys[key] = struct{}{}
}

func (s *ClientState) drainWatchedKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.watchedKeys))
	for key := range s.watchedKeys {
		keys = append(keys, key)
	}
	clear(s.watchedKeys)
	return keys
}

// DrainTransaction returns the queued commands and exits the transaction state.
func (s *ClientState) DrainTransaction() []QueuedCommand {
	s.mu.Lock()
	defer s.mu.Unlock()

	queued := s.TxQueue
	s.InTransaction = false
	s.TxQueue = nil
	return queued
}

// ResetTransaction clears queued transaction state.
func (s *ClientState) ResetTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.InTransaction = false
	s.TxFailed = false
	s.TxQueue = nil
}

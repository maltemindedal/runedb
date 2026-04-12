package server

import (
	"bytes"
	"context"
	"sync"
)

// QueuedCommand stores command metadata queued for transactional EXEC.
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

	Authenticated     bool
	Replica           bool
	ReplicaListenPort int
	LastWriteOffset   int64
	InTransaction     bool
	TxFailed          bool
	TxDirty           bool
	TxQueue           []QueuedCommand
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
	s.TxFailed = false
	s.TxDirty = false
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

// SetReplicaListeningPort records the port announced during REPLCONF listening-port.
func (s *ClientState) SetReplicaListeningPort(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ReplicaListenPort = port
}

// ReplicaListeningPort returns the port announced by the replica, if any.
func (s *ClientState) ReplicaListeningPort() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.ReplicaListenPort
}

// PromoteToReplica marks the connection as a replica peer.
func (s *ClientState) PromoteToReplica() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Replica = true
}

// IsReplica reports whether the connection has completed replica handshake setup.
func (s *ClientState) IsReplica() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.Replica
}

// SetLastWriteReplicationOffset records the replication offset produced by the
// most recent write command issued by this client.
func (s *ClientState) SetLastWriteReplicationOffset(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.LastWriteOffset = offset
}

// LastWriteReplicationOffset returns the replication offset produced by the
// client's most recent write command.
func (s *ClientState) LastWriteReplicationOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.LastWriteOffset
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

// TransactionDirty reports whether queue-time validation has already failed.
func (s *ClientState) TransactionDirty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.TxDirty
}

// MarkTransactionDirty marks the current transaction as invalid due to queue-time errors.
func (s *ClientState) MarkTransactionDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TxDirty = true
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
	s.TxDirty = false
	s.TxQueue = nil
	return queued
}

// ResetTransaction clears queued transaction state.
func (s *ClientState) ResetTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.InTransaction = false
	s.TxFailed = false
	s.TxDirty = false
	s.TxQueue = nil
}

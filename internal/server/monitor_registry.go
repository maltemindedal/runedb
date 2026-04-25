package server

import (
	"sync"
	"sync/atomic"
)

// MonitorRegistry tracks clients currently receiving MONITOR events.
type MonitorRegistry struct {
	mu              sync.RWMutex
	subscriberCount atomic.Int64
	subscribers     map[uint64]*ClientState
}

// NewMonitorRegistry constructs an empty MONITOR subscriber registry.
func NewMonitorRegistry() *MonitorRegistry {
	return &MonitorRegistry{subscribers: make(map[uint64]*ClientState)}
}

// Subscribe registers state for MONITOR fan-out.
func (r *MonitorRegistry) Subscribe(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.subscribers[state.ID]; !exists {
		r.subscriberCount.Add(1)
	}
	r.subscribers[state.ID] = state
	state.setMonitoring(true)
}

// Unsubscribe removes state from MONITOR fan-out.
func (r *MonitorRegistry) Unsubscribe(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.subscribers[state.ID]; exists {
		delete(r.subscribers, state.ID)
		r.subscriberCount.Add(-1)
	}
	state.setMonitoring(false)
}

// AppendSubscribers appends a snapshot of monitor clients to dst.
func (r *MonitorRegistry) AppendSubscribers(dst []*ClientState) []*ClientState {
	if r == nil {
		return dst[:0]
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	dst = dst[:0]
	if cap(dst) < len(r.subscribers) {
		dst = make([]*ClientState, 0, len(r.subscribers))
	}
	for _, state := range r.subscribers {
		dst = append(dst, state)
	}

	return dst
}

// Count reports the number of clients currently in MONITOR mode.
func (r *MonitorRegistry) Count() int {
	if r == nil {
		return 0
	}

	return int(r.subscriberCount.Load())
}

// HasSubscribers reports whether any client is currently in MONITOR mode.
func (r *MonitorRegistry) HasSubscribers() bool {
	return r != nil && r.subscriberCount.Load() > 0
}

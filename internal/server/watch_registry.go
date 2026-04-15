package server

import "sync"

// WatchRegistry tracks optimistic-lock watchers for keys across all client states.
type WatchRegistry struct {
	mu       sync.RWMutex
	watchers map[string]map[uint64]*ClientState
}

// NewWatchRegistry constructs an empty optimistic-lock watch registry.
func NewWatchRegistry() *WatchRegistry {
	return &WatchRegistry{watchers: make(map[string]map[uint64]*ClientState)}
}

// Watch records that state should be invalidated when any of the supplied keys changes.
func (r *WatchRegistry) Watch(state *ClientState, keys ...string) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, key := range keys {
		if key == "" {
			continue
		}

		watchers := r.watchers[key]
		if watchers == nil {
			watchers = make(map[uint64]*ClientState)
			r.watchers[key] = watchers
		}

		watchers[state.ID] = state
		state.addWatchedKey(key)
	}
}

// UnwatchAll removes all watched keys for a client state.
func (r *WatchRegistry) UnwatchAll(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	keys := state.drainWatchedKeys()

	for _, key := range keys {
		watchers := r.watchers[key]
		if watchers == nil {
			continue
		}

		delete(watchers, state.ID)
		if len(watchers) == 0 {
			delete(r.watchers, key)
		}
	}

}

// Touch marks transactions as failed for any clients watching the supplied keys.
func (r *WatchRegistry) Touch(keys ...string) {
	if r == nil || len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		r.touchSingleKey(keys[0])
		return
	}

	states := make(map[uint64]*ClientState)

	r.mu.RLock()
	for _, key := range keys {
		watchers := r.watchers[key]
		for id, state := range watchers {
			states[id] = state
		}
	}
	r.mu.RUnlock()

	for _, state := range states {
		state.MarkTransactionFailed()
	}
}

func (r *WatchRegistry) touchSingleKey(key string) {
	r.mu.RLock()
	watchers := r.watchers[key]
	states := make([]*ClientState, 0, len(watchers))
	for _, state := range watchers {
		states = append(states, state)
	}
	r.mu.RUnlock()

	for _, state := range states {
		state.MarkTransactionFailed()
	}
}

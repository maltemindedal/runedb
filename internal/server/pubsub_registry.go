package server

import "sync"

// PubSubRegistry tracks active exact-channel subscriptions across all client states.
type PubSubRegistry struct {
	mu          sync.RWMutex
	subscribers map[string]map[uint64]*ClientState
}

// NewPubSubRegistry constructs an empty pub/sub registry.
func NewPubSubRegistry() *PubSubRegistry {
	return &PubSubRegistry{subscribers: make(map[string]map[uint64]*ClientState)}
}

// Subscribe records that state is listening on channel.
func (r *PubSubRegistry) Subscribe(state *ClientState, channel string) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	subscribers := r.subscribers[channel]
	if subscribers == nil {
		subscribers = make(map[uint64]*ClientState)
		r.subscribers[channel] = subscribers
	}

	subscribers[state.ID] = state
	state.addSubscribedChannel(channel)
}

// Unsubscribe removes state from channel.
func (r *PubSubRegistry) Unsubscribe(state *ClientState, channel string) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	subscribers := r.subscribers[channel]
	if subscribers != nil {
		delete(subscribers, state.ID)
		if len(subscribers) == 0 {
			delete(r.subscribers, channel)
		}
	}

	state.removeSubscribedChannel(channel)
}

// UnsubscribeAll removes every active pub/sub subscription for state.
func (r *PubSubRegistry) UnsubscribeAll(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	channels := state.drainSubscribedChannels()
	for _, channel := range channels {
		subscribers := r.subscribers[channel]
		if subscribers == nil {
			continue
		}

		delete(subscribers, state.ID)
		if len(subscribers) == 0 {
			delete(r.subscribers, channel)
		}
	}
}

// Subscribers returns a snapshot of the clients currently subscribed to channel.
func (r *PubSubRegistry) Subscribers(channel string) []*ClientState {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	subscribers := r.subscribers[channel]
	states := make([]*ClientState, 0, len(subscribers))
	for _, state := range subscribers {
		states = append(states, state)
	}

	return states
}

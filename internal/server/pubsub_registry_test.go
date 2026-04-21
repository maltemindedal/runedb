package server

import "testing"

func TestPubSubRegistryMaintainsClientStateInvariants(t *testing.T) {
	t.Run("duplicate subscribe keeps counts stable and snapshots detached", func(t *testing.T) {
		registry := NewPubSubRegistry()
		state := &ClientState{ID: 1, Authenticated: true}
		state.SetPubSubRegistry(registry)

		registry.Subscribe(state, "updates")
		registry.Subscribe(state, "updates")

		if got := state.SubscriptionCount(); got != 1 {
			t.Fatalf("SubscriptionCount() = %d, want 1", got)
		}
		snapshot := registry.Subscribers("updates")
		if len(snapshot) != 1 {
			t.Fatalf("len(Subscribers(updates)) = %d, want 1", len(snapshot))
		}

		snapshot[0] = nil
		fresh := registry.Subscribers("updates")
		if len(fresh) != 1 {
			t.Fatalf("len(fresh Subscribers(updates)) = %d, want 1", len(fresh))
		}
		if fresh[0] != state {
			t.Fatalf("fresh snapshot[0] = %p, want %p", fresh[0], state)
		}

		registry.Unsubscribe(state, "missing")
		if got := state.SubscriptionCount(); got != 1 {
			t.Fatalf("SubscriptionCount() after unsubscribing missing channel = %d, want 1", got)
		}
	})

	t.Run("unsubscribe all removes every channel without mutating prior snapshots", func(t *testing.T) {
		registry := NewPubSubRegistry()
		state := &ClientState{ID: 2, Authenticated: true}
		state.SetPubSubRegistry(registry)

		registry.Subscribe(state, "alpha")
		registry.Subscribe(state, "beta")

		alphaSnapshot := registry.Subscribers("alpha")
		registry.UnsubscribeAll(state)

		if got := state.SubscriptionCount(); got != 0 {
			t.Fatalf("SubscriptionCount() after UnsubscribeAll = %d, want 0", got)
		}
		if got := len(registry.Subscribers("alpha")); got != 0 {
			t.Fatalf("len(Subscribers(alpha)) after UnsubscribeAll = %d, want 0", got)
		}
		if got := len(registry.Subscribers("beta")); got != 0 {
			t.Fatalf("len(Subscribers(beta)) after UnsubscribeAll = %d, want 0", got)
		}
		if len(alphaSnapshot) != 1 || alphaSnapshot[0] != state {
			t.Fatalf("alpha snapshot = %#v, want original subscriber entry intact", alphaSnapshot)
		}
	})
}

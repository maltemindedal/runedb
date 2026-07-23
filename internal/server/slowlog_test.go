package server

import (
	"fmt"
	"testing"
	"time"
)

func TestSlowlogRegistry(t *testing.T) {
	t.Run("retains entries newest first and clones commands", func(t *testing.T) {
		registry := NewSlowlogRegistry()
		command := []string{"SET", "name", "Stash"}
		registry.Record(SlowlogEntry{Timestamp: time.Unix(1, 0), Duration: time.Millisecond, Command: command})
		command[1] = "mutated"

		entries := registry.Entries(-1)
		if len(entries) != 1 {
			t.Fatalf("len(entries) = %d, want 1", len(entries))
		}
		if entries[0].ID != 0 {
			t.Fatalf("entry ID = %d, want 0", entries[0].ID)
		}
		if got := entries[0].Command[1]; got != "name" {
			t.Fatalf("command token = %q, want cloned original", got)
		}

		entries[0].Command[1] = "changed-again"
		if got := registry.Entries(-1)[0].Command[1]; got != "name" {
			t.Fatalf("stored command token = %q after caller mutation, want name", got)
		}
	})

	t.Run("wraps at fixed capacity", func(t *testing.T) {
		registry := NewSlowlogRegistry()
		for i := 0; i < defaultSlowlogCapacity+3; i++ {
			registry.Record(SlowlogEntry{Command: []string{"PING", fmt.Sprintf("%d", i)}})
		}

		if got := registry.Len(); got != defaultSlowlogCapacity {
			t.Fatalf("Len() = %d, want %d", got, defaultSlowlogCapacity)
		}
		entries := registry.Entries(2)
		if len(entries) != 2 {
			t.Fatalf("len(Entries(2)) = %d, want 2", len(entries))
		}
		if entries[0].ID != int64(defaultSlowlogCapacity+2) || entries[1].ID != int64(defaultSlowlogCapacity+1) {
			t.Fatalf("entry IDs = %d,%d, want newest retained IDs", entries[0].ID, entries[1].ID)
		}
	})

	t.Run("reset clears entries", func(t *testing.T) {
		registry := NewSlowlogRegistry()
		registry.Record(SlowlogEntry{Command: []string{"PING"}})
		registry.Reset()

		if got := registry.Len(); got != 0 {
			t.Fatalf("Len() = %d after Reset, want 0", got)
		}
		if got := len(registry.Entries(-1)); got != 0 {
			t.Fatalf("len(Entries) = %d after Reset, want 0", got)
		}

		registry.Record(SlowlogEntry{Command: []string{"PING"}})
		entries := registry.Entries(-1)
		if len(entries) != 1 {
			t.Fatalf("len(Entries) = %d after post-reset record, want 1", len(entries))
		}
		if got := entries[0].ID; got != 1 {
			t.Fatalf("post-reset entry ID = %d, want monotonic ID 1", got)
		}
	})
}

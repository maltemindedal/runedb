package server

import (
	"context"
	"io"
	"testing"

	"log/slog"

	"github.com/maltemindedal/runedb/internal/config"
	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/storage"
)

type stubExecutor struct{}

func (stubExecutor) Execute(context.Context, protocol.Value) (protocol.Value, error) {
	return protocol.SimpleString{Value: "OK"}, nil
}

func (stubExecutor) ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error) {
	return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
}

type stubWatchExecutor struct {
	registry *WatchRegistry
}

func (s stubWatchExecutor) Execute(context.Context, protocol.Value) (protocol.Value, error) {
	return protocol.SimpleString{Value: "OK"}, nil
}

func (s stubWatchExecutor) ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error) {
	return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
}

func (s stubWatchExecutor) WatchRegistry() *WatchRegistry {
	return s.registry
}

func TestClientStateLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.RequirePass = "secret"

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, logger, storage.NewStore(), stubExecutor{})

	first := srv.createClientState(1)
	second := srv.createClientState(2)
	if first == nil {
		t.Fatal("createClientState(1) returned nil state")
		return
	}
	if second == nil {
		t.Fatal("createClientState(2) returned nil state")
		return
	}
	if first == second {
		t.Fatal("createClientState() returned the same state for distinct clients")
	}
	if first.Authenticated {
		t.Fatal("first.Authenticated = true, want false when requirepass is configured")
	}
	if second.Authenticated {
		t.Fatal("second.Authenticated = true, want false when requirepass is configured")
	}

	if got := srv.getClientState(1); got != first {
		t.Fatalf("getClientState(1) = %p, want %p", got, first)
	}

	ctx := WithClientState(context.Background(), first)
	restored, ok := ClientStateFromContext(ctx)
	if !ok {
		t.Fatal("ClientStateFromContext() ok = false, want true")
	}
	if restored != first {
		t.Fatalf("ClientStateFromContext() = %p, want %p", restored, first)
	}

	srv.removeClientState(1)
	if got := srv.getClientState(1); got != nil {
		t.Fatalf("getClientState(1) after remove = %p, want nil", got)
	}
	if got := srv.getClientState(2); got != second {
		t.Fatalf("getClientState(2) = %p, want %p", got, second)
	}

	srv.clearClientStates()
	if got := srv.getClientState(2); got != nil {
		t.Fatalf("getClientState(2) after clear = %p, want nil", got)
	}
}

func TestServerClientStateCleanupUnwatchesKeys(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := NewWatchRegistry()
	srv := New(config.Default(), logger, storage.NewStore(), stubWatchExecutor{registry: registry})

	state := srv.createClientState(1)
	state.WatchKeys("alpha")

	srv.removeClientState(1)
	registry.Touch("alpha")

	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true, want false after cleanup removed all watchers")
	}

	other := srv.createClientState(2)
	other.WatchKeys("beta")
	srv.clearClientStates()
	registry.Touch("beta")

	if other.TransactionFailed() {
		t.Fatal("TransactionFailed() for cleared state = true, want false after clearClientStates cleanup")
	}
}

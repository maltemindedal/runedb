package server

import (
	"context"
	"io"
	"testing"

	"log/slog"

	"github.com/maltemindedal/godis/internal/config"
	"github.com/maltemindedal/godis/internal/protocol"
	"github.com/maltemindedal/godis/internal/storage"
)

type stubExecutor struct{}

func (stubExecutor) Execute(context.Context, protocol.Value) (protocol.Value, error) {
	return protocol.SimpleString{Value: "OK"}, nil
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

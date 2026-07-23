package aof_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
)

func TestLoadFile(t *testing.T) {
	t.Run("replays valid RESP commands", func(t *testing.T) {
		path := writeTempAOF(t, mustEncodeValues(t,
			request("SET", "name", "Stash"),
			request("INCR", "counter"),
		))

		var replayed []string
		stats, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
			req, decodeErr := command.DecodeRequest(value)
			if decodeErr != nil {
				return decodeErr
			}
			replayed = append(replayed, req.Name)
			return nil
		})
		if err != nil {
			t.Fatalf("LoadFile() error = %v", err)
		}
		if stats.ReplayedCommands != 2 {
			t.Fatalf("stats.ReplayedCommands = %d, want 2", stats.ReplayedCommands)
		}
		if stats.TruncatedTail {
			t.Fatal("stats.TruncatedTail = true, want false")
		}
		if len(replayed) != 2 || replayed[0] != "SET" || replayed[1] != "INCR" {
			t.Fatalf("replayed = %v, want [SET INCR]", replayed)
		}
	})

	t.Run("treats truncated tail as recoverable", func(t *testing.T) {
		full := mustEncodeValues(t,
			request("SET", "name", "Stash"),
			request("INCR", "counter"),
		)
		path := writeTempAOF(t, full[:len(full)-3])

		stats, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
			_, decodeErr := command.DecodeRequest(value)
			return decodeErr
		})
		if err != nil {
			t.Fatalf("LoadFile() error = %v, want truncated tail recovery", err)
		}
		if stats.ReplayedCommands != 1 {
			t.Fatalf("stats.ReplayedCommands = %d, want 1", stats.ReplayedCommands)
		}
		if !stats.TruncatedTail {
			t.Fatal("stats.TruncatedTail = false, want true")
		}
	})

	t.Run("fails on corrupt middle payload", func(t *testing.T) {
		path := writeTempAOF(t, append(mustEncodeValues(t, request("PING")), []byte("not-resp")...))

		_, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
			_, decodeErr := command.DecodeRequest(value)
			return decodeErr
		})
		if err == nil {
			t.Fatal("LoadFile() error = nil, want parse failure")
		}
	})

	t.Run("honors canceled context", func(t *testing.T) {
		path := writeTempAOF(t, mustEncodeValues(t, request("SET", "name", "Stash")))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := aof.LoadFile(ctx, path, func(_ context.Context, value protocol.Value) error {
			_, decodeErr := command.DecodeRequest(value)
			return decodeErr
		})
		if err == nil {
			t.Fatal("LoadFile() error = nil, want canceled context failure")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("LoadFile() error = %v, want context.Canceled", err)
		}
	})
}

func writeTempAOF(t *testing.T, payload []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	return path
}

func mustEncodeValues(t *testing.T, values ...protocol.Value) []byte {
	t.Helper()
	payload, err := protocol.EncodeValues(values)
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	return payload
}

func request(parts ...string) protocol.Value {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	return protocol.Array{Elements: elements}
}

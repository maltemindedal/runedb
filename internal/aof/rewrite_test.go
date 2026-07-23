package aof_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestGenerateRewriteRoundTripsState(t *testing.T) {
	store := storage.NewStore()
	store.Set("name", []byte("Stash"), 0)
	store.Set("expiring", []byte("soon"), time.Now().Add(time.Minute).UnixMilli())
	if _, err := store.RightPush("letters", [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	if _, err := store.HSet("profile", []storage.HashFieldValue{{Field: "lang", Value: []byte("go")}, {Field: "tier", Value: []byte("senior")}}); err != nil {
		t.Fatalf("HSet() error = %v", err)
	}
	if _, err := store.SAdd("tags", [][]byte{[]byte("fast"), []byte("durable")}); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}
	if _, err := store.ZAdd("leaders", []storage.ZSetEntry{{Member: []byte("alpha"), Score: 1}, {Member: []byte("beta"), Score: 2}}); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}
	if _, err := store.XAdd("events", "1-0", [][]byte{[]byte("type"), []byte("start")}); err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}

	entries, _ := store.SnapshotAll()
	var payload bytes.Buffer
	stats, err := aof.GenerateRewrite(entries, &payload)
	if err != nil {
		t.Fatalf("GenerateRewrite() error = %v", err)
	}
	if stats.Keys != 7 {
		t.Fatalf("stats.Keys = %d, want 7", stats.Keys)
	}
	if stats.Commands != 7 {
		t.Fatalf("stats.Commands = %d, want 7", stats.Commands)
	}

	replayedStore := storage.NewStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	executor := command.NewExecutor(replayedStore, logger)
	parser := protocol.NewParser(bytes.NewReader(payload.Bytes()))
	for {
		value, parseErr := parser.Parse()
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				break
			}
			t.Fatalf("Parse() error = %v", parseErr)
		}
		if _, execErr := executor.ExecuteDetailed(context.Background(), value); execErr != nil {
			t.Fatalf("ExecuteDetailed() error = %v", execErr)
		}
	}

	if got, ok, err := replayedStore.Get("name"); err != nil {
		t.Fatalf("Get(name) error = %v", err)
	} else if !ok || string(got) != "Stash" {
		t.Fatalf("Get(name) = (%q, %v), want (Stash, true)", string(got), ok)
	}
	if got, ok, err := replayedStore.Get("expiring"); err != nil {
		t.Fatalf("Get(expiring) error = %v", err)
	} else if !ok || string(got) != "soon" {
		t.Fatalf("Get(expiring) = (%q, %v), want (soon, true)", string(got), ok)
	}

	replayedEntries, _ := replayedStore.SnapshotAll()
	var foundTTL bool
	for _, entry := range replayedEntries {
		if entry.Key == "expiring" {
			foundTTL = entry.ExpiresAt > time.Now().UnixMilli()
		}
	}
	if !foundTTL {
		t.Fatal("rewritten expiring key did not retain a future TTL")
	}

	if values, err := replayedStore.ListRange("letters", 0, -1); err != nil {
		t.Fatalf("ListRange() error = %v", err)
	} else if len(values) != 2 || string(values[0]) != "a" || string(values[1]) != "b" {
		t.Fatalf("ListRange() = %q, want [a b]", values)
	}
	if fields, err := replayedStore.HGetAll("profile"); err != nil {
		t.Fatalf("HGetAll() error = %v", err)
	} else if len(fields) != 2 {
		t.Fatalf("len(HGetAll()) = %d, want 2", len(fields))
	}
	if members, err := replayedStore.SMembers("tags"); err != nil {
		t.Fatalf("SMembers() error = %v", err)
	} else if len(members) != 2 {
		t.Fatalf("len(SMembers()) = %d, want 2", len(members))
	}
	if entries, err := replayedStore.ZRange("leaders", 0, -1); err != nil {
		t.Fatalf("ZRange() error = %v", err)
	} else if len(entries) != 2 || entries[0].Member != "alpha" || entries[1].Member != "beta" {
		t.Fatalf("ZRange() = %#v, want alpha/beta order", entries)
	}
	if entries, err := replayedStore.XRead("events", "0-0"); err != nil {
		t.Fatalf("XRead() error = %v", err)
	} else if len(entries) != 1 || entries[0].ID != "1-0" {
		t.Fatalf("XRead() = %#v, want single entry 1-0", entries)
	}
}

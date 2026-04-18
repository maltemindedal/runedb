package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStoreValueBehavior(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *Store)
	}{
		{
			name: "Set/Get copies input value",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				payload := []byte("hello")
				store.Set("greeting", payload, 0)
				payload[0] = 'H'

				got, ok, err := store.Get("greeting")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !ok {
					t.Fatal("Get() ok = false, want true")
				}
				if string(got) != "hello" {
					t.Fatalf("Get() value = %q, want %q", string(got), "hello")
				}
			},
		},
		{
			name: "Get returns defensive copy",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				store.Set("greeting", []byte("hello"), 0)

				got, ok, err := store.Get("greeting")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !ok {
					t.Fatal("Get() ok = false, want true")
				}

				got[0] = 'H'

				again, ok, err := store.Get("greeting")
				if err != nil {
					t.Fatalf("second Get() error = %v", err)
				}
				if !ok {
					t.Fatal("second Get() ok = false, want true")
				}
				if string(again) != "hello" {
					t.Fatalf("second Get() value = %q, want %q", string(again), "hello")
				}
			},
		},
		{
			name: "Get passively evicts expired key",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				store.Set("expired", []byte("value"), time.Now().Add(-time.Millisecond).UnixMilli())

				if _, ok, err := store.Get("expired"); err != nil {
					t.Fatalf("Get() error = %v", err)
				} else if ok {
					t.Fatal("Get() ok = true, want false for expired key")
				}
				if store.Len() != 0 {
					t.Fatalf("Len() = %d, want 0 after passive eviction", store.Len())
				}
			},
		},
		{
			name: "Delete treats expired key as absent",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				store.Set("expired", []byte("value"), time.Now().Add(-time.Millisecond).UnixMilli())

				if ok := store.Delete("expired"); ok {
					t.Fatal("Delete() ok = true, want false for expired key")
				}
				if store.Len() != 0 {
					t.Fatalf("Len() = %d, want 0 after deleting expired key", store.Len())
				}
			},
		},
		{
			name: "Increment initializes and updates integer strings",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				first, err := store.Increment("counter")
				if err != nil {
					t.Fatalf("Increment() first error = %v", err)
				}
				if first != 1 {
					t.Fatalf("Increment() first = %d, want 1", first)
				}

				second, err := store.Increment("counter")
				if err != nil {
					t.Fatalf("Increment() second error = %v", err)
				}
				if second != 2 {
					t.Fatalf("Increment() second = %d, want 2", second)
				}
			},
		},
		{
			name: "Increment rejects non-integer strings",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				store.Set("counter", []byte("hello"), 0)

				if _, err := store.Increment("counter"); err != ErrValueNotInteger {
					t.Fatalf("Increment() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name: "Get rejects list values with wrong type",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				if _, err := store.LeftPush("numbers", [][]byte{[]byte("one")}); err != nil {
					t.Fatalf("LeftPush() error = %v", err)
				}

				if _, _, err := store.Get("numbers"); err != ErrWrongType {
					t.Fatalf("Get() error = %v, want ErrWrongType", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			tt.run(t, store)
		})
	}
}

func TestStoreActiveEvictionRemovesExpiredKeys(t *testing.T) {
	store := NewStore()
	store.Set("a", []byte("1"), time.Now().Add(-time.Millisecond).UnixMilli())
	store.Set("b", []byte("2"), time.Now().Add(-time.Millisecond).UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.StartEviction(ctx, 5*time.Millisecond, 10)

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if store.Len() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("Len() = %d, want 0 after active eviction", store.Len())
}

func TestStoreSnapshotStrings(t *testing.T) {
	t.Run("returns defensive copies for supported string keys", func(t *testing.T) {
		store := NewStore()
		store.Set("name", []byte("RuneDB"), 0)

		entries, stats := store.SnapshotStrings()
		if stats.TotalKeys != 1 {
			t.Fatalf("stats.TotalKeys = %d, want 1", stats.TotalKeys)
		}
		if stats.ExportedKeys != 1 {
			t.Fatalf("stats.ExportedKeys = %d, want 1", stats.ExportedKeys)
		}
		if len(entries) != 1 {
			t.Fatalf("len(entries) = %d, want 1", len(entries))
		}

		entries[0].Value[0] = 'r'
		got, ok, err := store.Get("name")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if !ok {
			t.Fatal("Get() ok = false, want true")
		}
		if string(got) != "RuneDB" {
			t.Fatalf("Get() value = %q, want %q", string(got), "RuneDB")
		}
	})

	t.Run("skips expired and unsupported keys", func(t *testing.T) {
		store := NewStore()
		store.Set("alive", []byte("yes"), 0)
		store.Set("expired", []byte("gone"), time.Now().Add(-time.Millisecond).UnixMilli())
		if _, err := store.RightPush("jobs", [][]byte{[]byte("one")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		entries, stats := store.SnapshotStrings()
		if len(entries) != 1 || entries[0].Key != "alive" {
			t.Fatalf("SnapshotStrings() entries = %#v, want only alive string key", entries)
		}
		if stats.SkippedExpiredKeys != 1 {
			t.Fatalf("stats.SkippedExpiredKeys = %d, want 1", stats.SkippedExpiredKeys)
		}
		if stats.SkippedUnsupportedKeys != 1 {
			t.Fatalf("stats.SkippedUnsupportedKeys = %d, want 1", stats.SkippedUnsupportedKeys)
		}
	})
}

func TestStoreConcurrentAccess(t *testing.T) {
	store := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			for j := 0; j < 100; j++ {
				store.Set(key, []byte(strconv.Itoa(j)), 0)
				if _, ok, err := store.Get(key); err != nil {
					t.Errorf("Get(%q) error = %v", key, err)
					return
				} else if !ok {
					t.Errorf("Get(%q) ok = false, want true", key)
					return
				}
			}
		}()
	}

	wg.Wait()

	if store.Len() == 0 {
		t.Fatal("Len() = 0, want at least one stored key")
	}
}

func TestStoreListBehavior(t *testing.T) {
	t.Run("LeftPush and RightPush preserve Redis-style ordering", func(t *testing.T) {
		store := NewStore()

		length, err := store.LeftPush("letters", [][]byte{[]byte("a"), []byte("b")})
		if err != nil {
			t.Fatalf("LeftPush() error = %v", err)
		}
		if length != 2 {
			t.Fatalf("LeftPush() length = %d, want 2", length)
		}

		length, err = store.RightPush("letters", [][]byte{[]byte("c"), []byte("d")})
		if err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}
		if length != 4 {
			t.Fatalf("RightPush() length = %d, want 4", length)
		}

		values, err := store.ListRange("letters", 0, -1)
		if err != nil {
			t.Fatalf("ListRange() error = %v", err)
		}

		want := []string{"b", "a", "c", "d"}
		if len(values) != len(want) {
			t.Fatalf("len(ListRange()) = %d, want %d", len(values), len(want))
		}
		for i, got := range values {
			if string(got) != want[i] {
				t.Fatalf("ListRange()[%d] = %q, want %q", i, string(got), want[i])
			}
		}
	})

	t.Run("ListRange supports negative indexes", func(t *testing.T) {
		store := NewStore()
		if _, err := store.RightPush("numbers", [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		values, err := store.ListRange("numbers", -2, -1)
		if err != nil {
			t.Fatalf("ListRange() error = %v", err)
		}
		want := []string{"3", "4"}
		for i, got := range values {
			if string(got) != want[i] {
				t.Fatalf("ListRange()[%d] = %q, want %q", i, string(got), want[i])
			}
		}
	})

	t.Run("ListRange returns defensive copies", func(t *testing.T) {
		store := NewStore()
		if _, err := store.RightPush("letters", [][]byte{[]byte("a"), []byte("b")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		values, err := store.ListRange("letters", 0, -1)
		if err != nil {
			t.Fatalf("ListRange() error = %v", err)
		}

		values[0][0] = 'A'
		values[1] = []byte("changed")

		again, err := store.ListRange("letters", 0, -1)
		if err != nil {
			t.Fatalf("second ListRange() error = %v", err)
		}
		if got := string(again[0]); got != "a" {
			t.Fatalf("second ListRange()[0] = %q, want %q", got, "a")
		}
		if got := string(again[1]); got != "b" {
			t.Fatalf("second ListRange()[1] = %q, want %q", got, "b")
		}
	})

	t.Run("LeftPop removes head and clears empty list key", func(t *testing.T) {
		store := NewStore()
		if _, err := store.RightPush("jobs", [][]byte{[]byte("one")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		value, ok, err := store.LeftPop("jobs")
		if err != nil {
			t.Fatalf("LeftPop() error = %v", err)
		}
		if !ok || string(value) != "one" {
			t.Fatalf("LeftPop() = (%q, %v), want (%q, true)", string(value), ok, "one")
		}
		if store.Len() != 0 {
			t.Fatalf("Len() = %d, want 0 after popping final list element", store.Len())
		}
	})

	t.Run("List push notifies one waiter and unsubscribe stops notifications", func(t *testing.T) {
		store := NewStore()
		waiter := store.SubscribeListPush("events")

		if _, err := store.RightPush("events", [][]byte{[]byte("first")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		select {
		case <-waiter:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("waiter did not receive notification")
		}

		waiter = store.SubscribeListPush("events")
		store.UnsubscribeListPush("events", waiter)
		if _, err := store.RightPush("events", [][]byte{[]byte("second")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		select {
		case <-waiter:
			t.Fatal("received notification after unsubscribe")
		case <-time.After(50 * time.Millisecond):
		}
	})
}

func TestStoreSortedSetBehavior(t *testing.T) {
	t.Run("ZAdd and ZRange order by score then member", func(t *testing.T) {
		store := NewStore()

		added, err := store.ZAdd("leaders", []ZSetEntry{
			{Member: []byte("beta"), Score: 2},
			{Member: []byte("alpha"), Score: 1},
			{Member: []byte("aardvark"), Score: 2},
		})
		if err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}
		if added != 3 {
			t.Fatalf("ZAdd() added = %d, want 3", added)
		}

		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}

		wantMembers := []string{"alpha", "aardvark", "beta"}
		wantScores := []float64{1, 2, 2}
		if len(values) != len(wantMembers) {
			t.Fatalf("len(ZRange()) = %d, want %d", len(values), len(wantMembers))
		}
		for i, got := range values {
			if string(got.Member) != wantMembers[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, string(got.Member), wantMembers[i])
			}
			if got.Score != wantScores[i] {
				t.Fatalf("ZRange()[%d].Score = %v, want %v", i, got.Score, wantScores[i])
			}
		}
	})

	t.Run("ZAdd updates existing members without increasing added count", func(t *testing.T) {
		store := NewStore()
		if _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("alpha"), Score: 1}, {Member: []byte("beta"), Score: 2}}); err != nil {
			t.Fatalf("ZAdd() initial error = %v", err)
		}

		added, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("beta"), Score: 0.5}})
		if err != nil {
			t.Fatalf("ZAdd() update error = %v", err)
		}
		if added != 0 {
			t.Fatalf("ZAdd() added = %d, want 0", added)
		}

		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		want := []string{"beta", "alpha"}
		for i, got := range values {
			if string(got.Member) != want[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, string(got.Member), want[i])
			}
		}
	})

	t.Run("ZRange supports negative indexes", func(t *testing.T) {
		store := NewStore()
		if _, err := store.ZAdd("leaders", []ZSetEntry{
			{Member: []byte("alpha"), Score: 1},
			{Member: []byte("beta"), Score: 2},
			{Member: []byte("charlie"), Score: 3},
		}); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}

		values, err := store.ZRange("leaders", -2, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		want := []string{"beta", "charlie"}
		for i, got := range values {
			if string(got.Member) != want[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, string(got.Member), want[i])
			}
		}
	})

	t.Run("ZRange rejects wrong value type", func(t *testing.T) {
		store := NewStore()
		store.Set("leaders", []byte("hello"), 0)

		if _, err := store.ZRange("leaders", 0, -1); err != ErrWrongType {
			t.Fatalf("ZRange() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("ZAdd recreates expired key", func(t *testing.T) {
		store := NewStore()
		store.Set("leaders", []byte("stale"), time.Now().Add(-time.Millisecond).UnixMilli())

		added, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("fresh"), Score: 1}})
		if err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}
		if added != 1 {
			t.Fatalf("ZAdd() added = %d, want 1", added)
		}

		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		if len(values) != 1 || string(values[0].Member) != "fresh" {
			t.Fatalf("ZRange() = %#v, want fresh member", values)
		}
	})
}

func TestStoreStreamBehavior(t *testing.T) {
	t.Run("XAdd and XRead preserve append order and field values", func(t *testing.T) {
		store := NewStore()

		firstID, err := store.XAdd("events", "1-0", [][]byte{[]byte("type"), []byte("start")})
		if err != nil {
			t.Fatalf("XAdd() first error = %v", err)
		}
		if firstID != "1-0" {
			t.Fatalf("XAdd() first ID = %q, want %q", firstID, "1-0")
		}

		secondID, err := store.XAdd("events", "2-0", [][]byte{[]byte("type"), []byte("finish"), []byte("user"), []byte("42")})
		if err != nil {
			t.Fatalf("XAdd() second error = %v", err)
		}
		if secondID != "2-0" {
			t.Fatalf("XAdd() second ID = %q, want %q", secondID, "2-0")
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("len(XRead()) = %d, want 2", len(entries))
		}
		if entries[0].ID != "1-0" || entries[1].ID != "2-0" {
			t.Fatalf("XRead() IDs = [%q, %q], want [%q, %q]", entries[0].ID, entries[1].ID, "1-0", "2-0")
		}
		if got := string(entries[1].Values[0]); got != "type" {
			t.Fatalf("entries[1].Values[0] = %q, want %q", got, "type")
		}
		if got := string(entries[1].Values[1]); got != "finish" {
			t.Fatalf("entries[1].Values[1] = %q, want %q", got, "finish")
		}
		if got := string(entries[1].Values[2]); got != "user" {
			t.Fatalf("entries[1].Values[2] = %q, want %q", got, "user")
		}
		if got := string(entries[1].Values[3]); got != "42" {
			t.Fatalf("entries[1].Values[3] = %q, want %q", got, "42")
		}
	})

	t.Run("XAdd auto generated IDs increment sequence in same millisecond", func(t *testing.T) {
		stream := newStream()

		firstID, err := stream.add("*", [][]byte{[]byte("field"), []byte("one")}, 100)
		if err != nil {
			t.Fatalf("stream.add() first error = %v", err)
		}
		secondID, err := stream.add("*", [][]byte{[]byte("field"), []byte("two")}, 100)
		if err != nil {
			t.Fatalf("stream.add() second error = %v", err)
		}
		thirdID, err := stream.add("*", [][]byte{[]byte("field"), []byte("three")}, 101)
		if err != nil {
			t.Fatalf("stream.add() third error = %v", err)
		}

		if firstID != "100-0" {
			t.Fatalf("first auto ID = %q, want %q", firstID, "100-0")
		}
		if secondID != "100-1" {
			t.Fatalf("second auto ID = %q, want %q", secondID, "100-1")
		}
		if thirdID != "101-0" {
			t.Fatalf("third auto ID = %q, want %q", thirdID, "101-0")
		}
	})

	t.Run("XRead supports dollar special ID and bare millisecond IDs", func(t *testing.T) {
		store := NewStore()
		if _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("one")}); err != nil {
			t.Fatalf("XAdd() first error = %v", err)
		}
		if _, err := store.XAdd("events", "2-0", [][]byte{[]byte("field"), []byte("two")}); err != nil {
			t.Fatalf("XAdd() second error = %v", err)
		}

		entries, err := store.XRead("events", "$")
		if err != nil {
			t.Fatalf("XRead($) error = %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("len(XRead($)) = %d, want 0", len(entries))
		}

		entries, err = store.XRead("events", "1")
		if err != nil {
			t.Fatalf("XRead(1) error = %v", err)
		}
		if len(entries) != 1 || entries[0].ID != "2-0" {
			t.Fatalf("XRead(1) = %#v, want one entry with ID 2-0", entries)
		}
	})

	t.Run("XAdd rejects malformed or non monotonic IDs", func(t *testing.T) {
		store := NewStore()

		if _, err := store.XAdd("events", "not-an-id", [][]byte{[]byte("field"), []byte("value")}); err != ErrInvalidStreamID {
			t.Fatalf("XAdd() invalid ID error = %v, want ErrInvalidStreamID", err)
		}
		if _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
			t.Fatalf("XAdd() initial error = %v", err)
		}
		if _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != ErrStreamIDTooSmall {
			t.Fatalf("XAdd() non monotonic error = %v, want ErrStreamIDTooSmall", err)
		}
	})

	t.Run("XRead rejects wrong value type", func(t *testing.T) {
		store := NewStore()
		store.Set("events", []byte("plain"), 0)

		if _, err := store.XRead("events", "0-0"); err != ErrWrongType {
			t.Fatalf("XRead() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("XAdd recreates expired stream key", func(t *testing.T) {
		store := NewStore()
		store.setValueObjectForTest("events", newStreamValue(newStream(), time.Now().Add(-time.Millisecond).UnixMilli()))

		id, err := store.XAdd("events", "5-0", [][]byte{[]byte("field"), []byte("value")})
		if err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}
		if id != "5-0" {
			t.Fatalf("XAdd() ID = %q, want %q", id, "5-0")
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != 1 || entries[0].ID != "5-0" {
			t.Fatalf("XRead() = %#v, want single fresh entry", entries)
		}
	})

	t.Run("concurrent XAdd keeps stream IDs strictly increasing", func(t *testing.T) {
		store := NewStore()

		const writers = 32
		var wg sync.WaitGroup
		ids := make(chan string, writers)
		errs := make(chan error, writers)

		for i := 0; i < writers; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, err := store.XAdd("events", "*", [][]byte{[]byte("writer"), []byte(strconv.Itoa(i))})
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}()
		}

		wg.Wait()
		close(ids)
		close(errs)

		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent XAdd() error = %v", err)
			}
		}

		seen := map[string]struct{}{}
		for id := range ids {
			if _, ok := seen[id]; ok {
				t.Fatalf("duplicate ID generated: %q", id)
			}
			seen[id] = struct{}{}
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != writers {
			t.Fatalf("len(XRead()) = %d, want %d", len(entries), writers)
		}

		last := streamID{}
		for i, entry := range entries {
			current, err := parseStreamAddID(entry.ID)
			if err != nil {
				t.Fatalf("parseStreamAddID(%q) error = %v", entry.ID, err)
			}
			if i > 0 && compareStreamIDs(current, last) <= 0 {
				t.Fatalf("entry ID %q is not greater than previous ID %q", entry.ID, last.String())
			}
			last = current
		}
	})

	t.Run("concurrent XAdd and XRead do not race", func(t *testing.T) {
		store := NewStore()

		var wg sync.WaitGroup
		start := make(chan struct{})
		errCh := make(chan error, 2)

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 128; i++ {
				if _, err := store.XAdd("events", "*", [][]byte{[]byte("field"), []byte(strconv.Itoa(i))}); err != nil {
					errCh <- err
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 128; i++ {
				if _, err := store.XRead("events", "0-0"); err != nil {
					errCh <- err
					return
				}
			}
		}()

		close(start)
		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				t.Fatalf("concurrent stream access error = %v", err)
			}
		}
	})

	t.Run("XAdd copies caller provided values", func(t *testing.T) {
		store := NewStore()
		payload := [][]byte{[]byte("field"), []byte("value")}

		if _, err := store.XAdd("events", "1-0", payload); err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}
		payload[0][0] = 'F'
		payload[1][0] = 'V'

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if got := string(entries[0].Values[0]); got != "field" {
			t.Fatalf("entries[0].Values[0] = %q, want %q", got, "field")
		}
		if got := string(entries[0].Values[1]); got != "value" {
			t.Fatalf("entries[0].Values[1] = %q, want %q", got, "value")
		}
	})

	t.Run("XRead returns defensive copies", func(t *testing.T) {
		store := NewStore()
		if _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}

		entries[0].Values[0][0] = 'F'
		entries[0].Values[1] = []byte("changed")

		again, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("second XRead() error = %v", err)
		}
		if got := string(again[0].Values[0]); got != "field" {
			t.Fatalf("second XRead()[0].Values[0] = %q, want %q", got, "field")
		}
		if got := string(again[0].Values[1]); got != "value" {
			t.Fatalf("second XRead()[0].Values[1] = %q, want %q", got, "value")
		}
	})

	t.Run("XRead returns empty for missing stream", func(t *testing.T) {
		store := NewStore()

		entries, err := store.XRead("missing", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("len(XRead()) = %d, want 0", len(entries))
		}
	})

	t.Run("XRead rejects malformed IDs", func(t *testing.T) {
		store := NewStore()
		if _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}

		_, err := store.XRead("events", "bad-id")
		if err != ErrInvalidStreamID {
			t.Fatalf("XRead() error = %v, want ErrInvalidStreamID", err)
		}
	})

	t.Run("XAdd returns full millisecond sequence IDs", func(t *testing.T) {
		store := NewStore()
		id, err := store.XAdd("events", "*", [][]byte{[]byte("field"), []byte("value")})
		if err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}
		if parts := strings.Split(id, "-"); len(parts) != 2 {
			t.Fatalf("generated ID = %q, want <milliseconds>-<sequence>", id)
		}
	})
}

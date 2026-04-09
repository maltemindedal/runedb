package storage

import (
	"context"
	"fmt"
	"strconv"
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

				got, ok := store.Get("greeting")
				if !ok {
					t.Fatal("Get() ok = false, want true")
				}
				if string(got) != "hello" {
					t.Fatalf("Get() value = %q, want %q", string(got), "hello")
				}
			},
		},
		{
			name: "Get passively evicts expired key",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				store.Set("expired", []byte("value"), time.Now().Add(-time.Millisecond).UnixMilli())

				if _, ok := store.Get("expired"); ok {
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
				if _, ok := store.Get(key); !ok {
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

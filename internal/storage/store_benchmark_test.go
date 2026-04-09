package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkStore(b *testing.B) {
	payload := []byte("value")

	b.Run("Set same key", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			store.Set("hot", payload, 0)
		}
	})

	b.Run("Get hit", func(b *testing.B) {
		store := NewStore()
		store.Set("hot", payload, 0)
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, ok, err := store.Get("hot"); err != nil {
				b.Fatalf("Get() error = %v", err)
			} else if !ok {
				b.Fatal("Get() ok = false, want true")
			}
		}
	})

	b.Run("Get miss", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, ok, err := store.Get("missing"); err != nil {
				b.Fatalf("Get() error = %v", err)
			} else if ok {
				b.Fatal("Get() ok = true, want false")
			}
		}
	})

	b.Run("Parallel get hit", func(b *testing.B) {
		store := NewStore()
		store.Set("hot", payload, 0)
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, ok, err := store.Get("hot"); err != nil {
					b.Fatalf("Get() error = %v", err)
				} else if !ok {
					b.Fatal("Get() ok = false, want true")
				}
			}
		})
	})

	b.Run("Parallel set same key", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				store.Set("hot", payload, 0)
			}
		})
	})

	b.Run("Parallel mixed hot key", func(b *testing.B) {
		store := NewStore()
		store.Set("hot", payload, 0)
		b.ReportAllocs()

		var op atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				switch op.Add(1) % 3 {
				case 0:
					store.Set("hot", payload, 0)
				case 1:
					_, _, _ = store.Get("hot")
				default:
					_ = store.Delete("hot")
				}
			}
		})
	})

	b.Run("Parallel disjoint keys", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		var counter atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				key := fmt.Sprintf("key-%d", counter.Add(1)%128)
				store.Set(key, payload, 0)
				_, _, _ = store.Get(key)
			}
		})
	})

	b.Run("Parallel passive eviction", func(b *testing.B) {
		store := NewStore()
		store.Set("expired", payload, time.Now().Add(-time.Millisecond).UnixMilli())
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _ = store.Get("expired")
			}
		})
	})

	b.Run("Parallel active eviction with writes", func(b *testing.B) {
		store := NewStore()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.StartEviction(ctx, time.Millisecond, 32)
		b.ReportAllocs()

		var counter atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				index := counter.Add(1)
				key := fmt.Sprintf("ttl-%d", index%64)
				expiresAt := time.Now().Add(2 * time.Millisecond).UnixMilli()
				store.Set(key, payload, expiresAt)
				_, _, _ = store.Get(key)
			}
		})
	})
}

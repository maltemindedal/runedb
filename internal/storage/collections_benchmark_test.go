package storage

import (
	"fmt"
	"testing"
)

func BenchmarkHash(b *testing.B) {
	payload := []byte("value")

	b.Run("HSet new fields", func(b *testing.B) {
		pairs := make([]HashFieldValue, 16)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store := NewStore()
			if _, err := store.HSet("h", pairs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HSet overwrite", func(b *testing.B) {
		store := NewStore()
		pairs := make([]HashFieldValue, 16)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
		}
		if _, err := store.HSet("h", pairs); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.HSet("h", pairs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HGet hit", func(b *testing.B) {
		store := NewStore()
		if _, err := store.HSet("h", []HashFieldValue{{Field: "f", Value: payload}}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, _, err := store.HGet("h", "f"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HGetAll 64 fields", func(b *testing.B) {
		store := NewStore()
		pairs := make([]HashFieldValue, 64)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
		}
		if _, err := store.HSet("h", pairs); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.HGetAll("h"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSet(b *testing.B) {
	b.Run("SAdd unique batch", func(b *testing.B) {
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store := NewStore()
			if _, err := store.SAdd("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SAdd all duplicates", func(b *testing.B) {
		store := NewStore()
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		if _, err := store.SAdd("s", members); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SAdd("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SIsMember hit", func(b *testing.B) {
		store := NewStore()
		if _, err := store.SAdd("s", [][]byte{[]byte("m")}); err != nil {
			b.Fatal(err)
		}
		member := []byte("m")
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SIsMember("s", member); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SRem hit batch", func(b *testing.B) {
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store := NewStore()
			if _, err := store.SAdd("s", members); err != nil {
				b.Fatal(err)
			}
			if _, err := store.SRem("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SRem miss batch", func(b *testing.B) {
		store := NewStore()
		if _, err := store.SAdd("s", [][]byte{[]byte("present")}); err != nil {
			b.Fatal(err)
		}
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "absent%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SRem("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SMembers 64", func(b *testing.B) {
		store := NewStore()
		members := make([][]byte, 64)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		if _, err := store.SAdd("s", members); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SMembers("s"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

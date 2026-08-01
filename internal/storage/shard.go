package storage

import (
	"hash/maphash"
	"sync"
	"time"
)

const defaultShardCount = 256

// Shard owns a subset of the in-memory keyspace behind its own lock.
type Shard struct {
	mu   sync.RWMutex
	data map[string]*ValueObject
}

func newShards(count int) []Shard {
	if count <= 0 {
		count = defaultShardCount
	}

	shards := make([]Shard, count)
	for i := range shards {
		shards[i].data = make(map[string]*ValueObject)
	}

	return shards
}

// readKey applies fn to the live value stored at key while the key's shard read
// lock is held, and reports whether a live value was found.
//
// It owns everything a read of a single key must get right: taking one clock
// reading, locking the owning shard, treating a missing key and a key past its
// TTL alike, reclaiming the expired key on the way out, refreshing the access
// stamp, and releasing the lock on every path. fn therefore only ever sees a
// live value of unknown kind.
//
// fn runs under the read lock so it can copy whatever it needs before another
// goroutine can mutate the value; it must not retain the value, or memory
// borrowed from it, after it returns. A key that exists but fails fn reports
// found with the error, and does not have its access stamp refreshed: a read
// that could not be served is not an access.
func readKey[T any](s *Store, key string, fn func(value *ValueObject) (T, error)) (T, bool, error) {
	var zero T

	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return zero, false, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		s.dropIfStillExpired(shard, key)
		return zero, false, nil
	}
	defer shard.mu.RUnlock()

	result, err := fn(value)
	if err != nil {
		return zero, true, err
	}

	value.touch(now)
	return result, true, nil
}

func (s *Store) shardIndex(key string) int {
	return int(maphash.String(s.seed, key) % uint64(len(s.shards)))
}

func (s *Store) shardForKey(key string) *Shard {
	return &s.shards[s.shardIndex(key)]
}

func (s *Store) readLockAllShards() {
	for i := range s.shards {
		s.shards[i].mu.RLock()
	}
}

func (s *Store) readUnlockAllShards() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.RUnlock()
	}
}

func (s *Store) writeLockAllShards() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
}

func (s *Store) writeUnlockAllShards() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
}

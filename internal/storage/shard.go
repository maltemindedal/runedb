package storage

import (
	"hash/maphash"
	"sync"
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

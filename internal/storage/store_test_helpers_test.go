package storage

func (s *Store) setValueObjectForTest(key string, value *ValueObject) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	s.setKeyLocked(shard, key, value)
}

func (s *Store) expireKeyForTest(key string, expiresAt int64) bool {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if !ok {
		return false
	}

	value.ExpiresAt = expiresAt
	return true
}

func (s *Store) lastAccessedAtForTest(key string) int64 {
	shard := s.shardForKey(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	value, ok := shard.data[key]
	if !ok {
		return 0
	}

	return value.lastAccessed()
}

func (s *Store) valueObjectForTest(key string) *ValueObject {
	shard := s.shardForKey(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return shard.data[key]
}

package storage

func (s *Store) setValueObjectForTest(key string, value *ValueObject) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = value
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

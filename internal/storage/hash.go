package storage

import "time"

// HSet assigns one or more field/value pairs on the hash stored at key and
// returns the number of newly created fields. Existing fields are overwritten.
// When maxmemory is configured it first frees space and reports the keys
// evicted to make room; otherwise it evicts nothing.
func (s *Store) HSet(key string, pairs []HashFieldValue) (int64, []string, error) {
	if len(pairs) == 0 {
		return 0, nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	if s.maxMemoryEnabled() {
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()
		return s.hashSetLocked(key, pairs, now, true)
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return s.hashSetLocked(key, pairs, now, false)
}

// hashSetLocked requires the caller to hold the key's shard write lock, or all
// shard write locks when accounting is true (eviction touches other shards).
func (s *Store) hashSetLocked(key string, pairs []HashFieldValue, now int64, accounting bool) (int64, []string, error) {
	shard, current := s.prepareExistingValueLocked(key, now)

	var (
		newValue *ValueObject
		added    int64
		err      error
	)
	if current != nil {
		newValue = current
		if accounting {
			// Size the write against the pre-write value, and leave the hash
			// untouched if it turns out to breach maxmemory, by mutating a copy
			// rather than the value still stored under the key.
			newValue, err = current.cloneHashValue(current.ExpiresAt)
			if err != nil {
				return 0, nil, err
			}
		}
		added, err = newValue.hashSet(pairs)
		if err != nil {
			return 0, nil, err
		}
	} else {
		newValue = newHashValueForPairs(pairs, 0)
		newLen, lenErr := newValue.hashLen()
		if lenErr != nil {
			return 0, nil, lenErr
		}
		added = int64(newLen)
	}
	newValue.touch(now)

	if accounting {
		evicted, err := s.commitValueWithEvictionLocked(shard, key, current, newValue)
		if err != nil {
			return 0, nil, err
		}
		return added, evicted, nil
	}

	s.setKeyLocked(shard, key, newValue)
	return added, nil, nil
}

// HGet returns the value of the supplied field on the hash stored at key.
func (s *Store) HGet(key, field string) ([]byte, bool, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, false, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		s.dropIfStillExpired(shard, key)
		return nil, false, nil
	}
	raw, exists, err := value.hashGet(field)
	if err != nil {
		shard.mu.RUnlock()
		return nil, false, err
	}
	value.touch(now)
	if !exists {
		shard.mu.RUnlock()
		return nil, false, nil
	}
	cloned := cloneBytes(raw)
	shard.mu.RUnlock()

	return cloned, true, nil
}

// HDel removes the named fields from the hash stored at key and returns the
// number of fields actually removed. An empty hash is deleted from the store.
func (s *Store) HDel(key string, fields []string) (int64, error) {
	if len(fields) == 0 {
		return 0, ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		s.deleteKeyLocked(shard, key)
		ok = false
	}
	if !ok {
		return 0, nil
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	removed, err := value.hashDel(fields)
	if err != nil {
		return 0, err
	}

	value.touch(time.Now().UnixMilli())
	hashLen, err := value.hashLen()
	if err != nil {
		return 0, err
	}
	if hashLen == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return removed, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return removed, nil
}

// HGetAll returns every field/value pair on the hash stored at key. Order is
// not guaranteed and mirrors Redis semantics.
func (s *Store) HGetAll(key string) ([]HashFieldValue, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return []HashFieldValue{}, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		s.dropIfStillExpired(shard, key)
		return []HashFieldValue{}, nil
	}
	entries, err := value.hashEntries()
	if err != nil {
		shard.mu.RUnlock()
		return nil, err
	}
	value.touch(now)
	for i := range entries {
		entries[i].Value = cloneBytes(entries[i].Value)
	}
	shard.mu.RUnlock()

	return entries, nil
}

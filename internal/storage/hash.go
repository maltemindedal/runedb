package storage

import "time"

// HSet assigns one or more field/value pairs on the hash stored at key and
// returns the number of newly created fields. Existing fields are overwritten.
func (s *Store) HSet(key string, pairs []HashFieldValue) (int64, error) {
	if len(pairs) == 0 {
		return 0, ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	now := time.Now().UnixMilli()
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		s.deleteKeyLocked(shard, key)
		ok = false
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting && ok {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	var added int64
	if ok {
		var err error
		added, err = value.hashSet(pairs)
		if err != nil {
			return 0, err
		}
		value.touch(now)
		if accounting {
			newSize := s.approximateValueObjectSize(key, value)
			s.usedMemory.Add(newSize - oldSize)
		}
	} else {
		newValue := newHashValueForPairs(pairs, 0)
		newLen, err := newValue.hashLen()
		if err != nil {
			return 0, err
		}
		added = int64(newLen)
		s.setKeyLocked(shard, key, newValue)
		if accounting {
			s.usedMemory.Add(s.approximateValueObjectSize(key, newValue))
		}
	}

	return added, nil
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

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			s.deleteKeyLocked(shard, key)
		}
		shard.mu.Unlock()
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

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			s.deleteKeyLocked(shard, key)
		}
		shard.mu.Unlock()
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

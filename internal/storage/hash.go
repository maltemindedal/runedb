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
		delete(shard.data, key)
		ok = false
	}

	var fields map[string][]byte
	if ok {
		var err error
		fields, err = value.HashValue()
		if err != nil {
			return 0, err
		}
	} else {
		fields = make(map[string][]byte, len(pairs))
	}

	added := int64(0)
	for _, pair := range pairs {
		if _, exists := fields[pair.Field]; !exists {
			added++
		}
		fields[pair.Field] = cloneBytes(pair.Value)
	}

	if ok {
		value.LastAccessedAt = now
	} else {
		shard.data[key] = newHashValue(fields, 0)
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
			delete(shard.data, key)
		}
		shard.mu.Unlock()
		return nil, false, nil
	}
	fields, err := value.HashValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, false, err
	}
	raw, exists := fields[field]
	if !exists {
		shard.mu.RUnlock()
		return nil, false, nil
	}
	shard.mu.RUnlock()

	return cloneBytes(raw), true, nil
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
		delete(shard.data, key)
		ok = false
	}
	if !ok {
		return 0, nil
	}
	hash, err := value.HashValue()
	if err != nil {
		return 0, err
	}

	removed := int64(0)
	for _, field := range fields {
		if _, exists := hash[field]; exists {
			delete(hash, field)
			removed++
		}
	}

	value.LastAccessedAt = time.Now().UnixMilli()
	if len(hash) == 0 {
		delete(shard.data, key)
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
			delete(shard.data, key)
		}
		shard.mu.Unlock()
		return []HashFieldValue{}, nil
	}
	hash, err := value.HashValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, err
	}

	entries := make([]HashFieldValue, 0, len(hash))
	for field, raw := range hash {
		entries = append(entries, HashFieldValue{Field: field, Value: raw})
	}
	shard.mu.RUnlock()

	for i := range entries {
		entries[i].Value = cloneBytes(entries[i].Value)
	}

	return entries, nil
}

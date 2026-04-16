package storage

import "time"

// HSet assigns one or more field/value pairs on the hash stored at key and
// returns the number of newly created fields. Existing fields are overwritten.
func (s *Store) HSet(key string, pairs []HashFieldValue) (int64, error) {
	if len(pairs) == 0 {
		return 0, ErrSyntax
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	value, ok := s.data[key]
	if ok && isExpired(value, now) {
		delete(s.data, key)
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
		s.data[key] = newHashValue(fields, 0)
	}

	return added, nil
}

// HGet returns the value of the supplied field on the hash stored at key.
func (s *Store) HGet(key, field string) ([]byte, bool, error) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return nil, false, nil
	}
	if isExpired(value, now) {
		s.mu.RUnlock()

		s.mu.Lock()
		value, ok = s.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return nil, false, nil
	}
	fields, err := value.HashValue()
	if err != nil {
		s.mu.RUnlock()
		return nil, false, err
	}
	raw, exists := fields[field]
	if !exists {
		s.mu.RUnlock()
		return nil, false, nil
	}
	cloned := cloneBytes(raw)
	s.mu.RUnlock()

	return cloned, true, nil
}

// HDel removes the named fields from the hash stored at key and returns the
// number of fields actually removed. An empty hash is deleted from the store.
func (s *Store) HDel(key string, fields []string) (int64, error) {
	if len(fields) == 0 {
		return 0, ErrSyntax
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(s.data, key)
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
		delete(s.data, key)
	}

	return removed, nil
}

// HGetAll returns every field/value pair on the hash stored at key. Order is
// not guaranteed and mirrors Redis semantics.
func (s *Store) HGetAll(key string) ([]HashFieldValue, error) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return []HashFieldValue{}, nil
	}
	if isExpired(value, now) {
		s.mu.RUnlock()

		s.mu.Lock()
		value, ok = s.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return []HashFieldValue{}, nil
	}
	hash, err := value.HashValue()
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}

	entries := make([]HashFieldValue, 0, len(hash))
	for field, raw := range hash {
		entries = append(entries, HashFieldValue{Field: field, Value: cloneBytes(raw)})
	}
	s.mu.RUnlock()

	return entries, nil
}

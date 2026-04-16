package storage

import "time"

// SAdd adds the supplied members to the set stored at key and returns the
// number of newly added members.
func (s *Store) SAdd(key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
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

	var set map[string]struct{}
	if ok {
		var err error
		set, err = value.SetValue()
		if err != nil {
			return 0, err
		}
	} else {
		set = make(map[string]struct{}, len(members))
	}

	added := int64(0)
	for _, member := range members {
		// Compiler elides the string allocation for map[string(b)] in read
		// context, so duplicate members avoid the heap allocation entirely.
		if _, exists := set[string(member)]; exists {
			continue
		}
		set[string(member)] = struct{}{}
		added++
	}

	if ok {
		value.LastAccessedAt = now
	} else {
		s.data[key] = newSetValue(set, 0)
	}

	return added, nil
}

// SIsMember reports whether the supplied member is contained in the set stored at key.
func (s *Store) SIsMember(key string, member []byte) (bool, error) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return false, nil
	}
	if isExpired(value, now) {
		s.mu.RUnlock()

		s.mu.Lock()
		value, ok = s.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return false, nil
	}
	set, err := value.SetValue()
	if err != nil {
		s.mu.RUnlock()
		return false, err
	}
	_, exists := set[string(member)]
	s.mu.RUnlock()

	return exists, nil
}

// SRem removes the supplied members from the set stored at key and returns the
// number of members actually removed. An empty set is deleted from the store.
func (s *Store) SRem(key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
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
	set, err := value.SetValue()
	if err != nil {
		return 0, err
	}

	removed := int64(0)
	for _, member := range members {
		// Both map[string(b)] and delete(m, string(b)) are compiler-optimized
		// to avoid allocating a []byte->string copy.
		if _, exists := set[string(member)]; !exists {
			continue
		}
		delete(set, string(member))
		removed++
	}

	value.LastAccessedAt = time.Now().UnixMilli()
	if len(set) == 0 {
		delete(s.data, key)
	}

	return removed, nil
}

// SMembers returns every member of the set stored at key. Order is not
// guaranteed and mirrors Redis semantics.
func (s *Store) SMembers(key string) ([][]byte, error) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return [][]byte{}, nil
	}
	if isExpired(value, now) {
		s.mu.RUnlock()

		s.mu.Lock()
		value, ok = s.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return [][]byte{}, nil
	}
	set, err := value.SetValue()
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}

	members := make([][]byte, 0, len(set))
	for member := range set {
		members = append(members, []byte(member))
	}
	s.mu.RUnlock()

	return members, nil
}

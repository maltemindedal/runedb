package storage

import "time"

// SAdd adds the supplied members to the set stored at key and returns the
// number of newly added members.
func (s *Store) SAdd(key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
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
		added, err = value.setAdd(members)
		if err != nil {
			return 0, err
		}
		value.touch(now)
		if accounting {
			newSize := s.approximateValueObjectSize(key, value)
			s.usedMemory.Add(newSize - oldSize)
		}
	} else {
		newValue := newSetValueForMembers(members, 0)
		newLen, err := newValue.setLen()
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

// SIsMember reports whether the supplied member is contained in the set stored at key.
func (s *Store) SIsMember(key string, member []byte) (bool, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return false, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			s.deleteKeyLocked(shard, key)
		}
		shard.mu.Unlock()
		return false, nil
	}
	exists, err := value.setContains(member)
	if err != nil {
		shard.mu.RUnlock()
		return false, err
	}
	value.touch(now)
	shard.mu.RUnlock()

	return exists, nil
}

// SRem removes the supplied members from the set stored at key and returns the
// number of members actually removed. An empty set is deleted from the store.
func (s *Store) SRem(key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
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
	if !ok {
		return 0, nil
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	removed, err := value.setRemove(members)
	if err != nil {
		return 0, err
	}

	value.touch(now)
	setLen, err := value.setLen()
	if err != nil {
		return 0, err
	}
	if setLen == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return removed, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return removed, nil
}

// SMembers returns every member of the set stored at key. Order is not
// guaranteed and mirrors Redis semantics.
func (s *Store) SMembers(key string) ([][]byte, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return [][]byte{}, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			s.deleteKeyLocked(shard, key)
		}
		shard.mu.Unlock()
		return [][]byte{}, nil
	}
	members, err := value.setMembers()
	if err != nil {
		shard.mu.RUnlock()
		return nil, err
	}
	value.touch(now)
	shard.mu.RUnlock()

	return members, nil
}

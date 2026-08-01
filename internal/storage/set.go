package storage

import "time"

// SAdd adds the supplied members to the set stored at key and returns the
// number of newly added members. When maxmemory is configured it first frees
// space and reports the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) SAdd(key string, members [][]byte) (int64, []string, error) {
	if len(members) == 0 {
		return 0, nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	if s.maxMemoryEnabled() {
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()
		return s.setAddLocked(key, members, now, true)
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return s.setAddLocked(key, members, now, false)
}

// setAddLocked requires the caller to hold the key's shard write lock, or all
// shard write locks when accounting is true (eviction touches other shards).
func (s *Store) setAddLocked(key string, members [][]byte, now int64, accounting bool) (int64, []string, error) {
	shard, current := s.prepareExistingValueLocked(key, now)

	var (
		newValue *ValueObject
		added    int64
		err      error
	)
	if current != nil {
		newValue = current
		if accounting {
			// Size the write against the pre-write value, and leave the set
			// untouched if it turns out to breach maxmemory, by mutating a copy
			// rather than the value still stored under the key.
			newValue, err = current.cloneSetValue(current.ExpiresAt)
			if err != nil {
				return 0, nil, err
			}
		}
		added, err = newValue.setAdd(members)
		if err != nil {
			return 0, nil, err
		}
	} else {
		newValue = newSetValueForMembers(members, 0)
		newLen, lenErr := newValue.setLen()
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

		s.dropIfStillExpired(shard, key)
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

		s.dropIfStillExpired(shard, key)
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

package storage

import (
	"strconv"
	"time"
)

const (
	approxValueObjectOverhead int64 = 48
	approxCollectionOverhead  int64 = 32
	approxListEntryOverhead   int64 = 16
	approxHashEntryOverhead   int64 = 24
	approxSetEntryOverhead    int64 = 16
	approxZSetEntryOverhead   int64 = 24
	approxStreamEntryOverhead int64 = 32
	maxInt64Value                   = int64(^uint64(0) >> 1)
)

// ConfigureMaxMemory enables approximate keyspace accounting and probabilistic
// LRU eviction for subsequent writes. A limit of 0 disables the feature.
func (s *Store) ConfigureMaxMemory(limit int64, sampleSize int) {
	if s == nil {
		return
	}
	if limit < 0 {
		limit = 0
	}
	if sampleSize <= 0 {
		sampleSize = defaultSampleSize
	}

	s.writeLockAllShards()
	s.maxMemory.Store(limit)
	s.memoryEvictionSampleSize = sampleSize
	if limit > 0 {
		s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	} else {
		s.usedMemory.Store(0)
	}
	s.writeUnlockAllShards()
}

// MaxMemory returns the configured approximate keyspace memory limit in bytes.
func (s *Store) MaxMemory() int64 {
	if s == nil {
		return 0
	}

	return s.maxMemory.Load()
}

// UsedMemory returns the current approximate keyspace memory usage in bytes.
func (s *Store) UsedMemory() int64 {
	if s == nil {
		return 0
	}

	return s.usedMemory.Load()
}

// EnforceMaxMemory evicts least-recently-used candidates until the keyspace is
// at or below the configured maxmemory limit.
func (s *Store) EnforceMaxMemory() ([]string, error) {
	if !s.maxMemoryEnabled() {
		return nil, nil
	}

	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	limit := s.maxMemory.Load()
	s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	if s.usedMemory.Load() <= limit {
		return nil, nil
	}

	return s.evictUntilMemoryAtOrBelowLocked(limit, nil)
}

func (s *Store) SetWithEviction(key string, value []byte, expiresAt int64) ([]string, error) {
	if !s.maxMemoryEnabled() {
		s.Set(key, value, expiresAt)
		return nil, nil
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	oldSize := s.approximateValueObjectSize(key, current)
	newValue := newStringValue(value, expiresAt)
	newSize := s.approximateValueObjectSize(key, newValue)

	evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key))
	if err != nil {
		return nil, err
	}

	s.setKeyLocked(shard, key, newValue)
	s.usedMemory.Add(newSize - oldSize)
	return evicted, nil
}

func (s *Store) IncrementWithEviction(key string) (int64, []string, error) {
	if !s.maxMemoryEnabled() {
		value, err := s.Increment(key)
		return value, nil, err
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	if current == nil {
		newValue := newOwnedStringValue([]byte("1"), 0)
		newSize := s.approximateValueObjectSize(key, newValue)
		evicted, err := s.ensureMemoryAvailableLocked(newSize, protectedKeys(key))
		if err != nil {
			return 0, nil, err
		}

		s.setKeyLocked(shard, key, newValue)
		s.usedMemory.Add(newSize)
		return 1, evicted, nil
	}

	currentValue, err := current.StringValue()
	if err != nil {
		return 0, nil, err
	}
	parsed, err := strconv.ParseInt(string(currentValue), 10, 64)
	if err != nil || parsed == maxInt64Value {
		return 0, nil, ErrValueNotInteger
	}
	parsed++

	oldSize := s.approximateValueObjectSize(key, current)
	newValue := newOwnedStringValue([]byte(strconv.FormatInt(parsed, 10)), current.ExpiresAt)
	newValue.touch(now)
	newSize := s.approximateValueObjectSize(key, newValue)

	evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key))
	if err != nil {
		return 0, nil, err
	}

	s.setKeyLocked(shard, key, newValue)
	s.usedMemory.Add(newSize - oldSize)
	return parsed, evicted, nil
}

func (s *Store) LeftPushWithEviction(key string, values [][]byte) (int64, []string, error) {
	return s.pushListWithEviction(key, values, true)
}

func (s *Store) RightPushWithEviction(key string, values [][]byte) (int64, []string, error) {
	return s.pushListWithEviction(key, values, false)
}

func (s *Store) ZAddWithEviction(key string, entries []ZSetEntry) (int64, []string, error) {
	if !s.maxMemoryEnabled() {
		added, err := s.ZAdd(key, entries)
		return added, nil, err
	}
	if len(entries) == 0 {
		return 0, nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	var (
		newValue *ValueObject
		added    int64
		err      error
	)
	if current != nil {
		newValue, err = current.cloneZSetValue(current.ExpiresAt)
		if err != nil {
			return 0, nil, err
		}
		added, err = newValue.zsetAdd(entries)
		if err != nil {
			return 0, nil, err
		}
	} else {
		newValue = newZSetValueForEntries(entries, 0)
		newLen, err := newValue.zsetLen()
		if err != nil {
			return 0, nil, err
		}
		added = int64(newLen)
	}
	evicted, err := s.commitValueWithEvictionLocked(shard, key, current, newValue)
	if err != nil {
		return 0, nil, err
	}

	return added, evicted, nil
}

func (s *Store) XAddWithEviction(key, rawID string, values [][]byte) (string, []string, error) {
	if !s.maxMemoryEnabled() {
		id, err := s.XAdd(key, rawID, values)
		return id, nil, err
	}
	if len(values) == 0 || len(values)%2 != 0 {
		return "", nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	var (
		stream    *StreamValue
		expiresAt int64
	)
	if current != nil {
		var err error
		stream, err = current.StreamValue()
		if err != nil {
			return "", nil, err
		}
		expiresAt = current.ExpiresAt
		stream = cloneStreamValue(stream)
	} else {
		stream = newStream()
	}

	id, err := stream.add(rawID, values, now)
	if err != nil {
		return "", nil, err
	}

	newValue := newStreamValue(stream, expiresAt)
	evicted, err := s.commitValueWithEvictionLocked(shard, key, current, newValue)
	if err != nil {
		return "", nil, err
	}

	return id, evicted, nil
}

func (s *Store) HSetWithEviction(key string, pairs []HashFieldValue) (int64, []string, error) {
	if !s.maxMemoryEnabled() {
		added, err := s.HSet(key, pairs)
		return added, nil, err
	}
	if len(pairs) == 0 {
		return 0, nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	var (
		newValue *ValueObject
		added    int64
		err      error
	)
	if current != nil {
		newValue, err = current.cloneHashValue(current.ExpiresAt)
		if err != nil {
			return 0, nil, err
		}
		added, err = newValue.hashSet(pairs)
		if err != nil {
			return 0, nil, err
		}
	} else {
		newValue = newHashValueForPairs(pairs, 0)
		newLen, err := newValue.hashLen()
		if err != nil {
			return 0, nil, err
		}
		added = int64(newLen)
	}
	evicted, err := s.commitValueWithEvictionLocked(shard, key, current, newValue)
	if err != nil {
		return 0, nil, err
	}

	return added, evicted, nil
}

func (s *Store) SAddWithEviction(key string, members [][]byte) (int64, []string, error) {
	if !s.maxMemoryEnabled() {
		added, err := s.SAdd(key, members)
		return added, nil, err
	}
	if len(members) == 0 {
		return 0, nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	var (
		newValue *ValueObject
		added    int64
		err      error
	)
	if current != nil {
		newValue, err = current.cloneSetValue(current.ExpiresAt)
		if err != nil {
			return 0, nil, err
		}
		added, err = newValue.setAdd(members)
		if err != nil {
			return 0, nil, err
		}
	} else {
		newValue = newSetValueForMembers(members, 0)
		newLen, err := newValue.setLen()
		if err != nil {
			return 0, nil, err
		}
		added = int64(newLen)
	}
	evicted, err := s.commitValueWithEvictionLocked(shard, key, current, newValue)
	if err != nil {
		return 0, nil, err
	}

	return added, evicted, nil
}

func (s *Store) pushListWithEviction(key string, values [][]byte, left bool) (int64, []string, error) {
	if !s.maxMemoryEnabled() {
		length, err := s.pushList(key, values, left)
		return length, nil, err
	}
	if len(values) == 0 {
		return 0, nil, ErrSyntax
	}

	now := time.Now().UnixMilli()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, current := s.prepareExistingValueLocked(key, now)
	var (
		list      [][]byte
		expiresAt int64
	)
	if current != nil {
		currentList, err := current.ListValue()
		if err != nil {
			return 0, nil, err
		}
		list = append([][]byte(nil), currentList...)
		expiresAt = current.ExpiresAt
	}

	additions := cloneList(values)
	if left {
		combined := make([][]byte, len(list)+len(additions))
		for i := range additions {
			combined[i] = additions[len(additions)-1-i]
		}
		copy(combined[len(additions):], list)
		list = combined
	} else {
		list = append(list, additions...)
	}

	newValue := newListValue(list, expiresAt)
	evicted, err := s.commitValueWithEvictionLocked(shard, key, current, newValue)
	if err != nil {
		return 0, nil, err
	}

	s.waiters.notifyOne(key)
	return int64(len(list)), evicted, nil
}

func (s *Store) approximateValueObjectSize(key string, value *ValueObject) int64 {
	if value == nil {
		return 0
	}

	switch value.Kind {
	case ValueKindString:
		return s.approximateStringValueObjectSize(key, len(value.String), value.ExpiresAt)
	case ValueKindList:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead + int64(len(value.List))*approxListEntryOverhead
		for _, item := range value.List {
			size += int64(len(item))
		}
		return size
	case ValueKindHash:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.HashEncoding == ValueEncodingCompact {
			if value.CompactHash != nil {
				size += int64(len(value.CompactHash.entries))*approxHashEntryOverhead + int64(len(value.CompactHash.arena))
			}
			return size
		}
		size += int64(len(value.Hash)) * approxHashEntryOverhead
		for field, raw := range value.Hash {
			size += int64(len(field) + len(raw))
		}
		return size
	case ValueKindSet:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.SetEncoding == ValueEncodingCompact {
			if value.IntSet != nil {
				size += int64(value.IntSet.len()) * 8
			}
			return size
		}
		size += int64(len(value.Set)) * approxSetEntryOverhead
		for member := range value.Set {
			size += int64(len(member))
		}
		return size
	case ValueKindZSet:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.ZSetEncoding == ValueEncodingCompact {
			if value.CompactZSet != nil {
				size += int64(len(value.CompactZSet.entries))*approxZSetEntryOverhead + int64(len(value.CompactZSet.arena))
			}
			return size
		}
		if value.ZSet != nil {
			size += int64(len(value.ZSet.index)) * approxZSetEntryOverhead
			for member := range value.ZSet.index {
				size += int64(len(member))
			}
		}
		return size
	case ValueKindStream:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.Stream != nil {
			size += int64(len(value.Stream.entries)) * approxStreamEntryOverhead
			for _, record := range value.Stream.entries {
				size += int64(len(record.idText))
				for _, item := range record.values {
					size += int64(len(item))
				}
			}
		}
		return size
	}

	return approximateBaseValueObjectSize(key, value.ExpiresAt)
}

func (s *Store) approximateStringValueObjectSize(key string, length int, expiresAt int64) int64 {
	return approximateBaseValueObjectSize(key, expiresAt) + int64(length)
}

func approximateBaseValueObjectSize(key string, expiresAt int64) int64 {
	size := approxValueObjectOverhead + int64(len(key))
	if expiresAt > 0 {
		size += 8
	}
	return size
}

func (s *Store) commitValueWithEvictionLocked(shard *Shard, key string, oldValue *ValueObject, newValue *ValueObject) ([]string, error) {
	oldSize := s.approximateValueObjectSize(key, oldValue)
	newSize := s.approximateValueObjectSize(key, newValue)

	evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key))
	if err != nil {
		return nil, err
	}

	s.setKeyLocked(shard, key, newValue)
	s.usedMemory.Add(newSize - oldSize)
	return evicted, nil
}

func (s *Store) recalculateUsedMemoryLocked(now int64) int64 {
	used := int64(0)
	for i := range s.shards {
		shard := &s.shards[i]
		for key, value := range shard.data {
			if isExpired(value, now) {
				s.removeKeyLocked(shard, key)
				continue
			}
			used += s.approximateValueObjectSize(key, value)
		}
	}
	s.usedMemory.Store(used)
	return used
}

func (s *Store) ensureMemoryAvailableLocked(delta int64, protected map[string]struct{}) ([]string, error) {
	if !s.maxMemoryEnabled() || delta <= 0 {
		return nil, nil
	}

	current := s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	limit := s.maxMemory.Load()
	targetUsed := limit - delta
	if targetUsed < 0 {
		return nil, ErrMemoryLimitExceeded
	}
	if current <= targetUsed {
		return nil, nil
	}
	if current-s.totalEvictableMemoryLocked(protected) > targetUsed {
		return nil, ErrMemoryLimitExceeded
	}

	return s.evictUntilMemoryAtOrBelowLocked(targetUsed, protected)
}

func (s *Store) evictUntilMemoryAtOrBelowLocked(targetUsed int64, protected map[string]struct{}) ([]string, error) {
	evicted := make([]string, 0)
	for s.usedMemory.Load() > targetUsed {
		candidate := s.findStalestCandidateLocked(protected)
		if candidate == "" {
			break
		}

		if s.deleteKeyLocked(s.shardForKey(candidate), candidate) {
			evicted = append(evicted, candidate)
		}
	}

	if s.usedMemory.Load() > targetUsed {
		return nil, ErrMemoryLimitExceeded
	}

	return evicted, nil
}

func (s *Store) findStalestCandidateLocked(protected map[string]struct{}) string {
	sampled := s.sampleKeysLocked(s.memoryEvictionSampleSize)
	stalestKey := ""
	stalestAt := maxInt64Value
	for _, key := range sampled {
		if _, blocked := protected[key]; blocked {
			continue
		}
		value, ok := s.shardForKey(key).data[key]
		if !ok {
			continue
		}
		accessedAt := value.lastAccessed()
		if accessedAt < stalestAt {
			stalestAt = accessedAt
			stalestKey = key
		}
	}
	if stalestKey != "" {
		return stalestKey
	}

	for i := range s.shards {
		for key, value := range s.shards[i].data {
			if _, blocked := protected[key]; blocked {
				continue
			}
			accessedAt := value.lastAccessed()
			if accessedAt < stalestAt {
				stalestAt = accessedAt
				stalestKey = key
			}
		}
	}

	return stalestKey
}

func (s *Store) totalEvictableMemoryLocked(protected map[string]struct{}) int64 {
	total := int64(0)
	for i := range s.shards {
		for key, value := range s.shards[i].data {
			if _, blocked := protected[key]; blocked {
				continue
			}
			total += s.approximateValueObjectSize(key, value)
		}
	}

	return total
}

func (s *Store) sampleKeysLocked(limit int) []string {
	if limit <= 0 {
		return nil
	}

	keys := make([]string, 0, limit)
	start := int(time.Now().UnixNano() % int64(len(s.shards)))
	for offset := 0; offset < len(s.shards) && len(keys) < limit; offset++ {
		index := (start + offset) % len(s.shards)
		for key := range s.shards[index].data {
			keys = append(keys, key)
			if len(keys) == limit {
				break
			}
		}
	}

	return keys
}

func (s *Store) prepareExistingValueLocked(key string, now int64) (*Shard, *ValueObject) {
	shard := s.shardForKey(key)
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		s.deleteKeyLocked(shard, key)
		return shard, nil
	}

	return shard, value
}

func (s *Store) deleteKeyWithSizeLocked(shard *Shard, key string, size int64) bool {
	if shard == nil {
		return false
	}

	if _, ok := shard.data[key]; !ok {
		return false
	}
	if s.maxMemoryEnabled() {
		s.usedMemory.Add(-size)
	}
	s.removeKeyLocked(shard, key)
	return true
}

func (s *Store) deleteKeyLocked(shard *Shard, key string) bool {
	if shard == nil {
		return false
	}

	value, ok := shard.data[key]
	if !ok {
		return false
	}
	size := int64(0)
	if s.maxMemoryEnabled() {
		size = s.approximateValueObjectSize(key, value)
	}

	return s.deleteKeyWithSizeLocked(shard, key, size)
}

func (s *Store) maxMemoryEnabled() bool {
	return s != nil && s.maxMemory.Load() > 0
}

func protectedKeys(keys ...string) map[string]struct{} {
	if len(keys) == 0 {
		return nil
	}

	protected := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		protected[key] = struct{}{}
	}

	return protected
}

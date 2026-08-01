package storage

import (
	"math/bits"
	"time"
)

// MaxBitmapOffset is the largest supported Redis-compatible bitmap bit offset.
const MaxBitmapOffset = (1 << 32) - 1

// GetBit returns the bit stored at offset for a string value.
func (s *Store) GetBit(key string, offset int64) (int64, bool, error) {
	if err := validateBitmapOffset(offset); err != nil {
		return 0, false, err
	}

	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return 0, false, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		s.dropIfStillExpired(shard, key)
		return 0, false, nil
	}
	data, err := value.StringValue()
	if err != nil {
		shard.mu.RUnlock()
		return 0, true, err
	}
	value.touch(now)
	bit := bitAt(data, offset)
	shard.mu.RUnlock()

	return bit, true, nil
}

// SetBit sets the bit stored at offset for a string value and returns the
// previous bit. When maxmemory is configured it first frees space and reports
// the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) SetBit(key string, offset int64, bit int64) (int64, []string, error) {
	if err := validateBitmapArguments(offset, bit); err != nil {
		return 0, nil, err
	}

	return s.setBit(key, offset, bit)
}

func (s *Store) setBit(key string, offset int64, bit int64) (int64, []string, error) {
	now := time.Now().UnixMilli()
	// Decide the locking mode and accounting mode from a single read of the
	// maxmemory flag. Reading it again under a narrower lock would be a TOCTOU:
	// if accounting turned on in between, this call would take only one shard
	// lock yet enter the cross-shard recalculation path and touch other shards'
	// maps without holding their locks.
	accounting := s.maxMemoryEnabled()
	if accounting {
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()
		return s.setBitLocked(key, offset, bit, now, accounting)
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return s.setBitInShardLocked(shard, key, offset, bit, now, accounting)
}

func (s *Store) setBitLocked(key string, offset int64, bit int64, now int64, accounting bool) (int64, []string, error) {
	shard := s.shardForKey(key)
	return s.setBitInShardLocked(shard, key, offset, bit, now, accounting)
}

func (s *Store) setBitInShardLocked(shard *Shard, key string, offset int64, bit int64, now int64, accounting bool) (int64, []string, error) {
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		s.deleteKeyLocked(shard, key)
		ok = false
		value = nil
	}

	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}
	var data []byte
	var expiresAt int64
	if ok {
		current, err := value.StringValue()
		if err != nil {
			return 0, nil, err
		}
		data = current
		expiresAt = value.ExpiresAt
	}

	previous := bitAt(data, offset)
	byteIndex := int(offset / 8)
	mask := byte(1 << uint(7-(offset%8)))
	if ok && byteIndex < len(data) {
		setBitInByte(&value.String[byteIndex], mask, bit)
		value.touch(now)
		return previous, nil, nil
	}

	if accounting {
		newSize := s.approximateStringValueObjectSize(key, byteIndex+1, expiresAt)
		evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key))
		if err != nil {
			return 0, nil, err
		}

		extended := make([]byte, byteIndex+1)
		copy(extended, data)
		setBitInByte(&extended[byteIndex], mask, bit)
		newValue := newOwnedStringValue(extended, expiresAt)
		newValue.touch(now)
		s.setKeyLocked(shard, key, newValue)
		s.usedMemory.Add(newSize - oldSize)
		return previous, evicted, nil
	}

	extended := make([]byte, byteIndex+1)
	copy(extended, data)
	data = extended
	setBitInByte(&data[byteIndex], mask, bit)

	newValue := newOwnedStringValue(data, expiresAt)
	newValue.touch(now)
	s.setKeyLocked(shard, key, newValue)
	return previous, nil, nil
}

func validateBitmapArguments(offset int64, bit int64) error {
	if err := validateBitmapOffset(offset); err != nil {
		return err
	}
	if bit != 0 && bit != 1 {
		return ErrValueNotInteger
	}
	return nil
}

func validateBitmapOffset(offset int64) error {
	if offset < 0 || offset > MaxBitmapOffset {
		return ErrValueNotInteger
	}
	return nil
}

func setBitInByte(dst *byte, mask byte, bit int64) {
	if bit == 1 {
		*dst |= mask
	} else {
		*dst &^= mask
	}
}

// BitCount counts set bits in a string value, optionally over an inclusive byte range.
func (s *Store) BitCount(key string, start *int64, end *int64) (int64, bool, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return 0, false, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		s.dropIfStillExpired(shard, key)
		return 0, false, nil
	}
	data, err := value.StringValue()
	if err != nil {
		shard.mu.RUnlock()
		return 0, true, err
	}
	value.touch(now)
	count := countBits(data, start, end)
	shard.mu.RUnlock()

	return count, true, nil
}

func bitAt(data []byte, offset int64) int64 {
	byteIndex := int(offset / 8)
	if byteIndex >= len(data) {
		return 0
	}

	mask := byte(1 << uint(7-(offset%8)))
	if data[byteIndex]&mask == 0 {
		return 0
	}
	return 1
}

func countBits(data []byte, start *int64, end *int64) int64 {
	if len(data) == 0 {
		return 0
	}

	from, to, ok := normalizeBitmapRange(len(data), start, end)
	if !ok {
		return 0
	}

	count := int64(0)
	for _, b := range data[from : to+1] {
		count += int64(bits.OnesCount8(b))
	}
	return count
}

func normalizeBitmapRange(length int, start *int64, end *int64) (int, int, bool) {
	if start == nil || end == nil {
		return 0, length - 1, true
	}

	from := *start
	to := *end
	if from < 0 {
		from = int64(length) + from
	}
	if to < 0 {
		to = int64(length) + to
	}
	if from < 0 {
		from = 0
	}
	if to < 0 || from >= int64(length) || from > to {
		return 0, 0, false
	}
	if to >= int64(length) {
		to = int64(length) - 1
	}

	return int(from), int(to), true
}

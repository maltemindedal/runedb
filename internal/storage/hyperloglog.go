package storage

import (
	"bytes"
	"hash/fnv"
	"math"
	"math/bits"
	"time"
)

// HyperLogLog values are stored as string values with a fixed dense layout:
// a 16-byte header (magic + encoding version) followed by one byte per
// register. The representation never grows after creation, which keeps every
// HyperLogLog key at a fixed bounded footprint.
const (
	hllIndexBits     = 14
	hllRegisterCount = 1 << hllIndexBits
	hllHeaderSize    = 16
	// HyperLogLogValueSize is the fixed byte length of a stored HyperLogLog value.
	HyperLogLogValueSize = hllHeaderSize + hllRegisterCount
	hllDenseEncoding     = 1
	hllMaxRank           = 64 - hllIndexBits + 1
)

var hllMagic = []byte{'H', 'Y', 'L', 'L'}

// PFAdd registers elements into a HyperLogLog value and reports whether the
// cardinality estimate changed.
func (s *Store) PFAdd(key string, elements [][]byte) (int64, error) {
	changed, _, err := s.pfAdd(key, elements)
	return changed, err
}

// PFAddWithEviction registers elements and evicts keys first if maxmemory requires it.
func (s *Store) PFAddWithEviction(key string, elements [][]byte) (int64, []string, error) {
	return s.pfAdd(key, elements)
}

func (s *Store) pfAdd(key string, elements [][]byte) (int64, []string, error) {
	now := time.Now().UnixMilli()
	if s.maxMemoryEnabled() {
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()
		return s.pfAddInShardLocked(s.shardForKey(key), key, elements, now)
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return s.pfAddInShardLocked(shard, key, elements, now)
}

func (s *Store) pfAddInShardLocked(shard *Shard, key string, elements [][]byte, now int64) (int64, []string, error) {
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		s.deleteKeyLocked(shard, key)
		ok = false
		value = nil
	}

	if !ok {
		payload := newHyperLogLogPayload()
		var evicted []string
		if s.maxMemoryEnabled() {
			newSize := s.approximateStringValueObjectSize(key, len(payload), 0)
			var err error
			evicted, err = s.ensureMemoryAvailableLocked(newSize, protectedKeys(key))
			if err != nil {
				return 0, nil, err
			}
			s.usedMemory.Add(newSize)
		}
		updateHyperLogLogRegisters(payload[hllHeaderSize:], elements)
		newValue := newOwnedStringValue(payload, 0)
		newValue.touch(now)
		s.setKeyLocked(shard, key, newValue)
		return 1, evicted, nil
	}

	data, err := value.StringValue()
	if err != nil {
		return 0, nil, err
	}
	registers, err := hyperLogLogRegisters(data)
	if err != nil {
		return 0, nil, err
	}
	changed := updateHyperLogLogRegisters(registers, elements)
	value.touch(now)
	return changed, nil, nil
}

// PFCount returns the approximate cardinality of one HyperLogLog key or of
// the union of multiple HyperLogLog keys. Missing keys count as empty.
func (s *Store) PFCount(keys []string) (int64, error) {
	merged := make([]byte, hllRegisterCount)
	for _, key := range keys {
		registers, ok, err := s.hyperLogLogRegistersForKey(key)
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}
		for i, rank := range registers {
			if merged[i] < rank {
				merged[i] = rank
			}
		}
	}

	return hyperLogLogEstimate(merged), nil
}

func (s *Store) hyperLogLogRegistersForKey(key string) ([]byte, bool, error) {
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
	data, err := value.StringValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, true, err
	}
	registers, err := hyperLogLogRegisters(data)
	if err != nil {
		shard.mu.RUnlock()
		return nil, true, err
	}
	value.touch(now)
	copied := cloneBytes(registers)
	shard.mu.RUnlock()

	return copied, true, nil
}

func newHyperLogLogPayload() []byte {
	payload := make([]byte, HyperLogLogValueSize)
	copy(payload, hllMagic)
	payload[len(hllMagic)] = hllDenseEncoding
	return payload
}

func hyperLogLogRegisters(data []byte) ([]byte, error) {
	if len(data) != HyperLogLogValueSize || !bytes.Equal(data[:len(hllMagic)], hllMagic) || data[len(hllMagic)] != hllDenseEncoding {
		return nil, ErrNotHyperLogLog
	}
	return data[hllHeaderSize:], nil
}

func updateHyperLogLogRegisters(registers []byte, elements [][]byte) int64 {
	changed := int64(0)
	for _, element := range elements {
		index, rank := hyperLogLogPosition(element)
		if registers[index] < rank {
			registers[index] = rank
			changed = 1
		}
	}
	return changed
}

// hyperLogLogPosition hashes an element into a register index taken from the
// top hllIndexBits bits and a rank derived from the remaining bits. The hash
// must stay deterministic across process restarts because stored registers
// are replayed through AOF and RDB persistence. FNV-1a alone distributes
// similar short keys poorly across the high index bits, so the sum is passed
// through the MurmurHash3 64-bit finalizer for avalanche.
func hyperLogLogPosition(element []byte) (int, byte) {
	hasher := fnv.New64a()
	_, _ = hasher.Write(element)
	sum := mix64(hasher.Sum64())

	index := int(sum >> (64 - hllIndexBits))
	rank := bits.LeadingZeros64(sum<<hllIndexBits) + 1
	if rank > hllMaxRank {
		rank = hllMaxRank
	}
	return index, byte(rank)
}

// mix64 is the MurmurHash3 64-bit finalizer.
func mix64(sum uint64) uint64 {
	sum ^= sum >> 33
	sum *= 0xff51afd7ed558ccd
	sum ^= sum >> 33
	sum *= 0xc4ceb9fe1a85ec53
	sum ^= sum >> 33
	return sum
}

// hyperLogLogEstimate applies the standard HyperLogLog estimator with the
// linear-counting correction for small cardinalities.
func hyperLogLogEstimate(registers []byte) int64 {
	m := float64(hllRegisterCount)
	alpha := 0.7213 / (1 + 1.079/m)

	sum := 0.0
	zeros := 0
	for _, rank := range registers {
		sum += 1 / float64(uint64(1)<<rank)
		if rank == 0 {
			zeros++
		}
	}

	estimate := alpha * m * m / sum
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}
	return int64(math.Round(estimate))
}

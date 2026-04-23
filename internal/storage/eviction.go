package storage

import (
	"context"
	"fmt"
	"time"
)

const defaultSampleSize = 20

// StartEviction launches the background TTL eviction loop.
func (s *Store) StartEviction(ctx context.Context, interval time.Duration, sampleSize int) {
	if interval <= 0 {
		return
	}
	if sampleSize <= 0 {
		sampleSize = defaultSampleSize
	}

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logError("background eviction loop panicked", "panic", fmt.Sprint(recovered))
			}
		}()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		s.logDebug("background eviction loop started", "interval", interval, "sample_size", sampleSize)

		for {
			select {
			case <-ctx.Done():
				s.logDebug("background eviction loop stopped", "reason", ctx.Err())
				return
			case <-ticker.C:
				removed := s.evictExpiredSample(time.Now().UnixMilli(), sampleSize)
				if removed > 0 {
					s.logDebug("background eviction removed expired keys", "removed", removed, "sample_size", sampleSize)
				}
			}
		}
	}()
}

func (s *Store) evictExpiredSample(now int64, sampleSize int) int {
	keys := s.snapshotKeys(sampleSize)
	if len(keys) == 0 {
		return 0
	}
	if len(keys) == 1 {
		shard := s.shardForKey(keys[0])
		shard.mu.Lock()
		defer shard.mu.Unlock()

		value, ok := shard.data[keys[0]]
		if ok && isExpired(value, now) {
			s.deleteKeyLocked(shard, keys[0])
			return 1
		}

		return 0
	}

	shardCount := len(s.shards)
	counts := make([]int, shardCount)
	for _, key := range keys {
		counts[s.shardIndex(key)]++
	}

	offsets := make([]int, shardCount)
	total := 0
	for shardID, count := range counts {
		offsets[shardID] = total
		total += count
	}

	groupedKeys := make([]string, len(keys))
	next := make([]int, shardCount)
	copy(next, offsets)
	for _, key := range keys {
		shardID := s.shardIndex(key)
		groupedKeys[next[shardID]] = key
		next[shardID]++
	}

	for shardID, count := range counts {
		if count == 0 {
			continue
		}
		s.shards[shardID].mu.Lock()
	}
	defer func() {
		for shardID := shardCount - 1; shardID >= 0; shardID-- {
			if counts[shardID] == 0 {
				continue
			}
			s.shards[shardID].mu.Unlock()
		}
	}()

	removed := 0
	for shardID, count := range counts {
		if count == 0 {
			continue
		}

		shard := &s.shards[shardID]
		start := offsets[shardID]
		for _, key := range groupedKeys[start : start+count] {
			value, ok := shard.data[key]
			if ok && isExpired(value, now) {
				s.deleteKeyLocked(shard, key)
				removed++
			}
		}
	}

	return removed
}

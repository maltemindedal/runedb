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

	groups := s.groupKeysByShard(keys)
	groups.lock(s)
	defer groups.unlock(s)

	removed := 0
	for shardID, count := range groups.counts {
		if count == 0 {
			continue
		}

		shard := &s.shards[shardID]
		for _, key := range groups.keysForShard(shardID) {
			value, ok := shard.data[key]
			if ok && isExpired(value, now) {
				s.deleteKeyLocked(shard, key)
				removed++
			}
		}
	}

	return removed
}

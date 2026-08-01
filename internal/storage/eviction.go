package storage

import (
	"context"
	"fmt"
	"time"
)

const defaultSampleSize = 20

// StartEviction launches the background TTL eviction loop, naming the keys each
// pass removed to onExpired. Active eviction is a keyspace mutation the store
// cannot publish on its own, so onExpired is where it becomes visible outside
// this process — as replication, durability, and WATCH invalidation.
//
// It returns a channel closed once the loop has exited, so a caller whose
// onExpired writes to resources it later tears down can order that teardown
// after the loop. Cancelling ctx stops the loop.
func (s *Store) StartEviction(ctx context.Context, interval time.Duration, sampleSize int, onExpired func(keys []string)) <-chan struct{} {
	done := make(chan struct{})
	if interval <= 0 {
		close(done)
		return done
	}
	if sampleSize <= 0 {
		sampleSize = defaultSampleSize
	}

	go func() {
		defer close(done)
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
				expired := s.evictExpiredSample(time.Now().UnixMilli(), sampleSize)
				if len(expired) == 0 {
					continue
				}

				s.logDebug("background eviction removed expired keys", "removed", len(expired), "sample_size", sampleSize)
				if onExpired != nil {
					// Reported once evictExpiredSample has released every shard
					// lock it took: onExpired reaches sinks outside the store,
					// which must never be entered while holding one.
					onExpired(expired)
				}
			}
		}
	}()

	return done
}

// evictExpiredSample removes the expired keys in one sample of the keyspace and
// returns them.
func (s *Store) evictExpiredSample(now int64, sampleSize int) []string {
	keys := s.snapshotKeys(sampleSize)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		shard := s.shardForKey(keys[0])
		shard.mu.Lock()
		defer shard.mu.Unlock()

		value, ok := shard.data[keys[0]]
		if ok && isExpired(value, now) {
			s.deleteKeyLocked(shard, keys[0])
			return keys
		}

		return nil
	}

	groups := s.groupKeysByShard(keys)
	groups.lock(s)
	defer groups.unlock(s)

	expired := make([]string, 0, len(keys))
	for shardID, count := range groups.counts {
		if count == 0 {
			continue
		}

		shard := &s.shards[shardID]
		for _, key := range groups.keysForShard(shardID) {
			value, ok := shard.data[key]
			if ok && isExpired(value, now) {
				s.deleteKeyLocked(shard, key)
				expired = append(expired, key)
			}
		}
	}

	return expired
}

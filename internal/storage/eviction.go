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

	removed := 0

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		value, ok := s.data[key]
		if ok && isExpired(value, now) {
			delete(s.data, key)
			removed++
		}
	}

	return removed
}

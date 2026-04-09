package storage

import (
	"context"
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
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.evictExpiredSample(time.Now().UnixMilli(), sampleSize)
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

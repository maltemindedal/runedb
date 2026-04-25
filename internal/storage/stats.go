package storage

import "time"

// KeyStats is an approximate snapshot of key counts by value kind.
type KeyStats struct {
	TotalKeys int
	ByKind    map[ValueKind]int
}

// KeyStats returns a snapshot of non-expired key counts by value kind.
func (s *Store) KeyStats() KeyStats {
	stats := KeyStats{ByKind: make(map[ValueKind]int)}
	if s == nil {
		return stats
	}

	now := time.Now().UnixMilli()
	s.readLockAllShards()
	defer s.readUnlockAllShards()

	for i := range s.shards {
		for _, value := range s.shards[i].data {
			if isExpired(value, now) {
				continue
			}
			stats.TotalKeys++
			stats.ByKind[value.Kind]++
		}
	}

	return stats
}

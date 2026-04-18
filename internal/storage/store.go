package storage

import (
	"errors"
	"hash/maphash"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrSyntax reports malformed storage-related command syntax.
	ErrSyntax = errors.New("syntax error")
	// ErrInvalidExpireTime reports an invalid EX/PX duration.
	ErrInvalidExpireTime = errors.New("invalid expire time in 'SET' command")
	// ErrInvalidStreamID reports that a stream ID could not be parsed.
	ErrInvalidStreamID = errors.New("invalid stream ID specified as stream command argument")
	// ErrStreamIDTooSmall reports that XADD was given a non-monotonic explicit ID.
	ErrStreamIDTooSmall = errors.New("stream ID is equal or smaller than the target stream top item")
	// ErrValueNotInteger reports that a stored string cannot be parsed as a 64-bit integer.
	ErrValueNotInteger = errors.New("value is not an integer or out of range")
	// ErrValueNotFloat reports that a command argument is not a valid float.
	ErrValueNotFloat = errors.New("value is not a valid float")
	// ErrWrongType reports that a command targeted the wrong logical value type.
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
)

// Store is a thread-safe in-memory key/value store.
type Store struct {
	shards   []Shard
	seed     maphash.Seed
	waiters  *listWaiters
	loggerMu sync.RWMutex
	logger   *slog.Logger
}

// NewStore constructs an empty Store.
func NewStore() *Store {
	return &Store{shards: newShards(defaultShardCount), seed: maphash.MakeSeed(), waiters: newListWaiters()}
}

// SetLogger configures optional structured logging for background store operations.
func (s *Store) SetLogger(logger *slog.Logger) {
	if s == nil {
		return
	}

	s.loggerMu.Lock()
	defer s.loggerMu.Unlock()
	s.logger = logger
}

// Set stores a byte slice under the provided key.
func (s *Store) Set(key string, value []byte, expiresAt int64) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = newStringValue(value, expiresAt)
}

// Get fetches a value from the store, passively evicting it if it has expired.
func (s *Store) Get(key string) ([]byte, bool, error) {
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
			delete(shard.data, key)
		}
		shard.mu.Unlock()
		return nil, false, nil
	}
	data, err := value.StringValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, true, err
	}
	cloned := cloneBytes(data)
	shard.mu.RUnlock()

	return cloned, true, nil
}

// Delete removes a key from the store.
func (s *Store) Delete(key string) bool {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		return false
	}

	delete(shard.data, key)
	return ok
}

// DeleteMany removes the supplied keys and returns the keys actually removed.
// Expired keys are treated as absent.
func (s *Store) DeleteMany(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		if s.Delete(keys[0]) {
			return []string{keys[0]}
		}
		return nil
	}

	keysByShard := make(map[int][]string, len(keys))
	shardIDs := make([]int, 0, len(keys))
	for _, key := range keys {
		shardID := s.shardIndex(key)
		if _, ok := keysByShard[shardID]; !ok {
			shardIDs = append(shardIDs, shardID)
		}
		keysByShard[shardID] = append(keysByShard[shardID], key)
	}

	slices.Sort(shardIDs)
	for _, shardID := range shardIDs {
		s.shards[shardID].mu.Lock()
	}
	defer func() {
		for i := len(shardIDs) - 1; i >= 0; i-- {
			s.shards[shardIDs[i]].mu.Unlock()
		}
	}()

	now := time.Now().UnixMilli()
	removed := make([]string, 0, len(keys))
	for _, shardID := range shardIDs {
		shard := &s.shards[shardID]
		for _, key := range keysByShard[shardID] {
			value, ok := shard.data[key]
			if !ok {
				continue
			}
			if isExpired(value, now) {
				delete(shard.data, key)
				continue
			}

			delete(shard.data, key)
			removed = append(removed, key)
		}
	}

	return removed
}

// Increment atomically increments the string value stored at key by one.
func (s *Store) Increment(key string) (int64, error) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	now := time.Now().UnixMilli()
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		delete(shard.data, key)
		ok = false
	}

	if !ok {
		shard.data[key] = newStringValue([]byte("1"), 0)
		return 1, nil
	}
	currentValue, err := value.StringValue()
	if err != nil {
		return 0, err
	}

	current, err := strconv.ParseInt(string(currentValue), 10, 64)
	if err != nil || current == math.MaxInt64 {
		return 0, ErrValueNotInteger
	}

	current++
	value.String = []byte(strconv.FormatInt(current, 10))
	value.LastAccessedAt = now
	return current, nil
}

// LeftPush prepends one or more values to the list stored at key and returns the new length.
func (s *Store) LeftPush(key string, values [][]byte) (int64, error) {
	return s.pushList(key, values, true)
}

// RightPush appends one or more values to the list stored at key and returns the new length.
func (s *Store) RightPush(key string, values [][]byte) (int64, error) {
	return s.pushList(key, values, false)
}

// LeftPop removes and returns the left-most value from the list stored at key.
func (s *Store) LeftPop(key string) ([]byte, bool, error) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	list, err := value.ListValue()
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		delete(shard.data, key)
		return nil, false, nil
	}

	item := list[0]
	list[0] = nil
	list = list[1:]
	value.List = list
	value.LastAccessedAt = time.Now().UnixMilli()
	if len(list) == 0 {
		delete(shard.data, key)
	}

	return item, true, nil
}

// RightPop removes and returns the right-most value from the list stored at key.
func (s *Store) RightPop(key string) ([]byte, bool, error) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	list, err := value.ListValue()
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		delete(shard.data, key)
		return nil, false, nil
	}

	last := len(list) - 1
	item := list[last]
	list[last] = nil
	list = list[:last]
	value.List = list
	value.LastAccessedAt = time.Now().UnixMilli()
	if len(list) == 0 {
		delete(shard.data, key)
	}

	return item, true, nil
}

// LeftPopN removes and returns up to count left-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) LeftPopN(key string, count int64) ([][]byte, bool, error) {
	return s.popN(key, count, true)
}

// RightPopN removes and returns up to count right-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) RightPopN(key string, count int64) ([][]byte, bool, error) {
	return s.popN(key, count, false)
}

func (s *Store) popN(key string, count int64, left bool) ([][]byte, bool, error) {
	if count < 0 {
		return nil, false, ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	list, err := value.ListValue()
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		delete(shard.data, key)
		return nil, false, nil
	}

	take := int64(len(list))
	if count < take {
		take = count
	}
	n := int(take)
	popped := make([][]byte, n)
	if left {
		for i := 0; i < n; i++ {
			popped[i] = list[i]
			list[i] = nil
		}
		list = list[n:]
	} else {
		for i := 0; i < n; i++ {
			src := len(list) - 1 - i
			popped[i] = list[src]
			list[src] = nil
		}
		list = list[:len(list)-n]
	}

	value.List = list
	value.LastAccessedAt = time.Now().UnixMilli()
	if len(list) == 0 {
		delete(shard.data, key)
	}

	return popped, true, nil
}

// ListRange returns an inclusive range of values from the list stored at key.
func (s *Store) ListRange(key string, start, stop int64) ([][]byte, error) {
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

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(shard.data, key)
		}
		shard.mu.Unlock()
		return [][]byte{}, nil
	}
	list, err := value.ListValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, err
	}

	from, to, ok := normalizeListRange(len(list), start, stop)
	if !ok {
		shard.mu.RUnlock()
		return [][]byte{}, nil
	}

	cloned := cloneList(list[from : to+1])
	shard.mu.RUnlock()
	return cloned, nil
}

// ZAdd inserts or updates one or more sorted-set members and returns the number of newly added members.
func (s *Store) ZAdd(key string, entries []ZSetEntry) (int64, error) {
	if len(entries) == 0 {
		return 0, ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		ok = false
	}

	var (
		set       *sortedSet
		expiresAt int64
	)
	if ok {
		var err error
		set, err = value.ZSetValue()
		if err != nil {
			return 0, err
		}
		expiresAt = value.ExpiresAt
	} else {
		set = newSortedSet()
	}

	added := int64(0)
	for _, entry := range entries {
		if set.add(string(entry.Member), entry.Score) {
			added++
		}
	}

	shard.data[key] = newZSetValue(set, expiresAt)
	return added, nil
}

// ZRange returns an inclusive rank range from the sorted set stored at key.
func (s *Store) ZRange(key string, start, stop int64) ([]ZSetRangeEntry, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return []ZSetRangeEntry{}, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(shard.data, key)
		}
		shard.mu.Unlock()
		return []ZSetRangeEntry{}, nil
	}
	set, err := value.ZSetValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, err
	}

	from, to, ok := normalizeListRange(set.len(), start, stop)
	if !ok {
		shard.mu.RUnlock()
		return []ZSetRangeEntry{}, nil
	}

	entries := set.rangeByRank(from, to)
	shard.mu.RUnlock()

	return entries, nil
}

// XAdd appends a new stream entry to the stream stored at key and returns its ID.
func (s *Store) XAdd(key, rawID string, values [][]byte) (string, error) {
	if len(values) == 0 || len(values)%2 != 0 {
		return "", ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		ok = false
	}

	var (
		stream    *streamValue
		expiresAt int64
	)
	if ok {
		var err error
		stream, err = value.StreamValue()
		if err != nil {
			return "", err
		}
		expiresAt = value.ExpiresAt
	} else {
		stream = newStream()
	}

	id, err := stream.add(rawID, values, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}

	shard.data[key] = newStreamValue(stream, expiresAt)
	return id, nil
}

// XRead returns stream entries whose IDs are greater than the supplied ID.
func (s *Store) XRead(key, rawID string) ([]StreamEntry, error) {
	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return []StreamEntry{}, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		shard.mu.Lock()
		value, ok = shard.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(shard.data, key)
		}
		shard.mu.Unlock()
		return []StreamEntry{}, nil
	}
	stream, err := value.StreamValue()
	if err != nil {
		shard.mu.RUnlock()
		return nil, err
	}

	entries, err := stream.readAfter(rawID)
	shard.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// SubscribeListPush registers a waiter that is notified when a push occurs for key.
func (s *Store) SubscribeListPush(key string) chan struct{} {
	return s.waiters.subscribe(key)
}

// UnsubscribeListPush removes a previously registered list push waiter.
func (s *Store) UnsubscribeListPush(key string, ch chan struct{}) {
	s.waiters.unsubscribe(key, ch)
}

// Len returns the current number of stored keys.
func (s *Store) Len() int {
	total := 0
	s.readLockAllShards()
	defer s.readUnlockAllShards()

	for i := range s.shards {
		total += len(s.shards[i].data)
	}

	return total
}

// SnapshotStrings returns defensive copies of the currently supported
// shutdown-persistence scope: non-expired DB 0 string keys only.
func (s *Store) SnapshotStrings() ([]StringSnapshotEntry, StringSnapshotStats) {
	now := time.Now().UnixMilli()

	s.readLockAllShards()
	defer s.readUnlockAllShards()

	totalKeys := 0
	for i := range s.shards {
		totalKeys += len(s.shards[i].data)
	}

	stats := StringSnapshotStats{TotalKeys: totalKeys}
	entries := make([]StringSnapshotEntry, 0, totalKeys)
	for i := range s.shards {
		for key, value := range s.shards[i].data {
			if isExpired(value, now) {
				stats.SkippedExpiredKeys++
				continue
			}
			if value.Kind != ValueKindString {
				stats.SkippedUnsupportedKeys++
				continue
			}

			entries = append(entries, StringSnapshotEntry{
				Key:       key,
				Value:     cloneBytes(value.String),
				ExpiresAt: value.ExpiresAt,
			})
			stats.ExportedKeys++
		}
	}

	return entries, stats
}

// ReplaceWith swaps the store contents with a snapshot from another store.
//
// The replacement is applied only after the source snapshot has already been
// materialized, which makes it suitable for FULLRESYNC-style state replacement.
func (s *Store) ReplaceWith(other *Store) {
	if s == nil || other == nil {
		return
	}

	other.readLockAllShards()
	replacement := newShards(len(s.shards))
	for i := range other.shards {
		for key, value := range other.shards[i].data {
			replacement[s.shardIndex(key)].data[key] = value
		}
	}
	other.readUnlockAllShards()

	s.writeLockAllShards()
	for i := range s.shards {
		s.shards[i].data = replacement[i].data
	}
	s.writeUnlockAllShards()
}

func (s *Store) logDebug(msg string, args ...any) {
	if s == nil {
		return
	}

	s.loggerMu.RLock()
	logger := s.logger
	s.loggerMu.RUnlock()
	if logger == nil {
		return
	}

	logger.Debug(msg, args...)
}

func (s *Store) logError(msg string, args ...any) {
	if s == nil {
		return
	}

	s.loggerMu.RLock()
	logger := s.logger
	s.loggerMu.RUnlock()
	if logger == nil {
		return
	}

	logger.Error(msg, args...)
}

func (s *Store) snapshotKeys(limit int) []string {
	if limit <= 0 {
		return nil
	}

	keys := make([]string, 0, limit)
	start := int(time.Now().UnixNano() % int64(len(s.shards)))
	for offset := 0; offset < len(s.shards) && len(keys) < limit; offset++ {
		index := (start + offset) % len(s.shards)
		shard := &s.shards[index]
		shard.mu.RLock()
		for key := range shard.data {
			keys = append(keys, key)
			if len(keys) == limit {
				break
			}
		}
		shard.mu.RUnlock()
	}

	return keys
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func cloneList(items [][]byte) [][]byte {
	if items == nil {
		return nil
	}

	cloned := make([][]byte, len(items))
	for i, item := range items {
		cloned[i] = cloneBytes(item)
	}

	return cloned
}

func (s *Store) pushList(key string, values [][]byte, left bool) (int64, error) {
	if len(values) == 0 {
		return 0, ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(shard.data, key)
		ok = false
	}

	var list [][]byte
	var expiresAt int64
	if ok {
		var err error
		list, err = value.ListValue()
		if err != nil {
			shard.mu.Unlock()
			return 0, err
		}
		expiresAt = value.ExpiresAt
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

	shard.data[key] = newListValue(list, expiresAt)
	newLen := int64(len(list))
	shard.mu.Unlock()

	s.waiters.notifyOne(key)
	return newLen, nil
}

func normalizeListRange(length int, start, stop int64) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}

	from := normalizeListIndex(length, start)
	to := normalizeListIndex(length, stop)
	if from < 0 {
		from = 0
	}
	if to < 0 {
		return 0, 0, false
	}
	if from >= length {
		return 0, 0, false
	}
	if to >= length {
		to = length - 1
	}
	if from > to {
		return 0, 0, false
	}

	return from, to, true
}

func normalizeListIndex(length int, index int64) int {
	if index < 0 {
		return length + int(index)
	}

	return int(index)
}

func isExpired(value *ValueObject, now int64) bool {
	return value != nil && value.ExpiresAt > 0 && now > value.ExpiresAt
}

func (s *Store) setValueObjectForTest(key string, value *ValueObject) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = value
}

func (s *Store) expireKeyForTest(key string, expiresAt int64) bool {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if !ok {
		return false
	}

	value.ExpiresAt = expiresAt
	return true
}

// ParseExpiryMillis parses Redis-style EX/PX arguments into a Unix-millis deadline.
func ParseExpiryMillis(args [][]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) != 2 {
		return 0, ErrSyntax
	}

	value, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || value <= 0 {
		return 0, ErrInvalidExpireTime
	}

	now := time.Now()
	switch strings.ToUpper(string(args[0])) {
	case "EX":
		return now.Add(time.Duration(value) * time.Second).UnixMilli(), nil
	case "PX":
		return now.Add(time.Duration(value) * time.Millisecond).UnixMilli(), nil
	default:
		return 0, ErrSyntax
	}
}

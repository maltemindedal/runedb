package storage

import (
	"errors"
	"math"
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
	mu      sync.RWMutex
	data    map[string]StoredValue
	waiters *listWaiters
}

// NewStore constructs an empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]StoredValue), waiters: newListWaiters()}
}

// Set stores a byte slice under the provided key.
func (s *Store) Set(key string, value []byte, expiresAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = newStringValue(value, expiresAt)
}

// Get fetches a value from the store, passively evicting it if it has expired.
func (s *Store) Get(key string) ([]byte, bool, error) {
	value, ok := s.loadValue(key)
	if !ok {
		return nil, false, nil
	}
	if value.Kind != ValueKindString {
		return nil, true, ErrWrongType
	}

	return value.String, true, nil
}

// Delete removes a key from the store.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(s.data, key)
		return false
	}

	delete(s.data, key)
	return ok
}

// Increment atomically increments the string value stored at key by one.
func (s *Store) Increment(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	value, ok := s.data[key]
	if ok && isExpired(value, now) {
		delete(s.data, key)
		ok = false
	}

	if !ok {
		s.data[key] = newStringValue([]byte("1"), 0)
		return 1, nil
	}
	if value.Kind != ValueKindString {
		return 0, ErrWrongType
	}

	current, err := strconv.ParseInt(string(value.String), 10, 64)
	if err != nil || current == math.MaxInt64 {
		return 0, ErrValueNotInteger
	}

	current++
	value.String = []byte(strconv.FormatInt(current, 10))
	s.data[key] = value
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
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(s.data, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	if value.Kind != ValueKindList {
		return nil, false, ErrWrongType
	}
	if len(value.List) == 0 {
		delete(s.data, key)
		return nil, false, nil
	}

	item := value.List[0]
	value.List[0] = nil
	value.List = value.List[1:]
	if len(value.List) == 0 {
		delete(s.data, key)
	} else {
		s.data[key] = value
	}

	return item, true, nil
}

// ListRange returns an inclusive range of values from the list stored at key.
func (s *Store) ListRange(key string, start, stop int64) ([][]byte, error) {
	value, ok := s.loadValue(key)
	if !ok {
		return [][]byte{}, nil
	}
	if value.Kind != ValueKindList {
		return nil, ErrWrongType
	}

	from, to, ok := normalizeListRange(len(value.List), start, stop)
	if !ok {
		return [][]byte{}, nil
	}

	return value.List[from : to+1], nil
}

// ZAdd inserts or updates one or more sorted-set members and returns the number of newly added members.
func (s *Store) ZAdd(key string, entries []ZSetEntry) (int64, error) {
	if len(entries) == 0 {
		return 0, ErrSyntax
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(s.data, key)
		ok = false
	}

	var (
		set       *sortedSet
		expiresAt int64
	)
	if ok {
		if value.Kind != ValueKindZSet {
			return 0, ErrWrongType
		}
		set = value.ZSet
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

	s.data[key] = newZSetValue(set, expiresAt)
	return added, nil
}

// ZRange returns an inclusive rank range from the sorted set stored at key.
func (s *Store) ZRange(key string, start, stop int64) ([]ZSetRangeEntry, error) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return []ZSetRangeEntry{}, nil
	}
	if isExpired(value, now) {
		s.mu.RUnlock()

		s.mu.Lock()
		value, ok = s.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return []ZSetRangeEntry{}, nil
	}
	if value.Kind != ValueKindZSet {
		s.mu.RUnlock()
		return nil, ErrWrongType
	}

	from, to, ok := normalizeListRange(value.ZSet.len(), start, stop)
	if !ok {
		s.mu.RUnlock()
		return []ZSetRangeEntry{}, nil
	}

	entries := value.ZSet.rangeByRank(from, to)
	s.mu.RUnlock()

	return entries, nil
}

// XAdd appends a new stream entry to the stream stored at key and returns its ID.
func (s *Store) XAdd(key, rawID string, values [][]byte) (string, error) {
	if len(values) == 0 || len(values)%2 != 0 {
		return "", ErrSyntax
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(s.data, key)
		ok = false
	}

	var (
		stream    *streamValue
		expiresAt int64
	)
	if ok {
		if value.Kind != ValueKindStream {
			return "", ErrWrongType
		}
		stream = value.Stream
		expiresAt = value.ExpiresAt
	} else {
		stream = newStream()
	}

	id, err := stream.add(rawID, values, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}

	s.data[key] = newStreamValue(stream, expiresAt)
	return id, nil
}

// XRead returns stream entries whose IDs are greater than the supplied ID.
func (s *Store) XRead(key, rawID string) ([]StreamEntry, error) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return []StreamEntry{}, nil
	}
	if isExpired(value, now) {
		s.mu.RUnlock()

		s.mu.Lock()
		value, ok = s.data[key]
		if ok && isExpired(value, time.Now().UnixMilli()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return []StreamEntry{}, nil
	}
	if value.Kind != ValueKindStream {
		s.mu.RUnlock()
		return nil, ErrWrongType
	}

	entries, err := value.Stream.readAfter(rawID)
	s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}

func (s *Store) snapshotKeys(limit int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > len(s.data) {
		limit = len(s.data)
	}

	keys := make([]string, 0, limit)
	for key := range s.data {
		keys = append(keys, key)
		if len(keys) == limit {
			break
		}
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

	s.mu.Lock()
	value, ok := s.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		delete(s.data, key)
		ok = false
	}

	var list [][]byte
	var expiresAt int64
	if ok {
		if value.Kind != ValueKindList {
			s.mu.Unlock()
			return 0, ErrWrongType
		}
		list = value.List
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

	s.data[key] = newListValue(list, expiresAt)
	newLen := int64(len(list))
	s.mu.Unlock()

	s.waiters.notifyOne(key)
	return newLen, nil
}

func (s *Store) loadValue(key string) (StoredValue, bool) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return StoredValue{}, false
	}

	if !isExpired(value, now) {
		return value, true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok = s.data[key]
	if !ok {
		return StoredValue{}, false
	}
	if !isExpired(value, time.Now().UnixMilli()) {
		return value, true
	}

	delete(s.data, key)
	return StoredValue{}, false
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

func isExpired(value StoredValue, now int64) bool {
	return value.ExpiresAt > 0 && now > value.ExpiresAt
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

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
	// ErrValueNotInteger reports that a stored string cannot be parsed as a 64-bit integer.
	ErrValueNotInteger = errors.New("value is not an integer or out of range")
	// ErrWrongType reports that a command targeted the wrong logical value type.
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
)

// Store is a thread-safe in-memory key/value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]StoredValue
}

// NewStore constructs an empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]StoredValue)}
}

// Set stores a byte slice under the provided key.
func (s *Store) Set(key string, value []byte, expiresAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = StoredValue{
		Data:      cloneBytes(value),
		ExpiresAt: expiresAt,
		Kind:      ValueKindString,
	}
}

// Get fetches a value from the store, passively evicting it if it has expired.
func (s *Store) Get(key string) ([]byte, bool) {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}

	if !isExpired(value, now) {
		return cloneBytes(value.Data), true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok = s.data[key]
	if !ok || !isExpired(value, time.Now().UnixMilli()) {
		if ok {
			return cloneBytes(value.Data), true
		}
		return nil, false
	}

	delete(s.data, key)
	return nil, false
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
		s.data[key] = StoredValue{Data: []byte("1"), Kind: ValueKindString}
		return 1, nil
	}
	if value.Kind != ValueKindString {
		return 0, ErrWrongType
	}

	current, err := strconv.ParseInt(string(value.Data), 10, 64)
	if err != nil || current == math.MaxInt64 {
		return 0, ErrValueNotInteger
	}

	current++
	value.Data = []byte(strconv.FormatInt(current, 10))
	s.data[key] = value
	return current, nil
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

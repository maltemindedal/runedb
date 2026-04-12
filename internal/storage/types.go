package storage

import "errors"

var errInvalidStoredValue = errors.New("storage: invalid stored value state")

// ValueKind identifies the logical data shape of a stored value.
type ValueKind string

const (
	// ValueKindString is the Phase 1 string storage type used by SET/GET.
	ValueKindString ValueKind = "string"
	// ValueKindList is the Phase 2 list storage type used by LPUSH/RPUSH/LRANGE.
	ValueKindList ValueKind = "list"
	// ValueKindZSet is the Phase 2 sorted-set storage type used by ZADD/ZRANGE.
	ValueKindZSet ValueKind = "zset"
	// ValueKindStream is the Phase 2 stream storage type used by XADD/XREAD.
	ValueKindStream ValueKind = "stream"
)

// ZSetEntry represents a member/score pair in a sorted set.
type ZSetEntry struct {
	Member []byte
	Score  float64
}

// ZSetRangeEntry represents a member/score pair returned from ZRANGE.
type ZSetRangeEntry struct {
	Member string
	Score  float64
}

// StreamEntry represents a stream entry returned by XREAD.
type StreamEntry struct {
	ID     string
	Values [][]byte
}

// StoredValue is the internal representation of an item stored in RuneDB.
//
// ExpiresAt is stored as a Unix timestamp in milliseconds. A value of 0 means
// the key does not expire.
type StoredValue struct {
	String    []byte
	List      [][]byte
	ZSet      *sortedSet
	Stream    *streamValue
	ExpiresAt int64
	Kind      ValueKind
}

// StringValue returns the string payload for a string value.
func (v StoredValue) StringValue() ([]byte, error) {
	if v.Kind != ValueKindString {
		return nil, ErrWrongType
	}

	return v.String, nil
}

// ListValue returns the list payload for a list value.
func (v StoredValue) ListValue() ([][]byte, error) {
	if v.Kind != ValueKindList {
		return nil, ErrWrongType
	}

	return v.List, nil
}

// ZSetValue returns the sorted-set payload for a sorted-set value.
func (v StoredValue) ZSetValue() (*sortedSet, error) {
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSet == nil {
		return nil, errInvalidStoredValue
	}

	return v.ZSet, nil
}

// StreamValue returns the stream payload for a stream value.
func (v StoredValue) StreamValue() (*streamValue, error) {
	if v.Kind != ValueKindStream {
		return nil, ErrWrongType
	}
	if v.Stream == nil {
		return nil, errInvalidStoredValue
	}

	return v.Stream, nil
}

func newStringValue(data []byte, expiresAt int64) StoredValue {
	return StoredValue{
		String:    cloneBytes(data),
		ExpiresAt: expiresAt,
		Kind:      ValueKindString,
	}
}

// newListValue stores an already-owned list representation without cloning.
// Callers must only pass slices whose element bytes are not aliased with caller-
// controlled memory.
func newListValue(items [][]byte, expiresAt int64) StoredValue {
	return StoredValue{
		List:      items,
		ExpiresAt: expiresAt,
		Kind:      ValueKindList,
	}
}

func newZSetValue(set *sortedSet, expiresAt int64) StoredValue {
	return StoredValue{
		ZSet:      set,
		ExpiresAt: expiresAt,
		Kind:      ValueKindZSet,
	}
}

func newStreamValue(stream *streamValue, expiresAt int64) StoredValue {
	return StoredValue{
		Stream:    stream,
		ExpiresAt: expiresAt,
		Kind:      ValueKindStream,
	}
}

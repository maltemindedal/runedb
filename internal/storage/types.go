package storage

import (
	"errors"
	"time"
)

var errInvalidValueObjectState = errors.New("storage: invalid value object state")

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
	// ValueKindHash is the Phase 6 hash storage type used by HSET/HGET/HDEL/HGETALL.
	ValueKindHash ValueKind = "hash"
	// ValueKindSet is the Phase 6 set storage type used by SADD/SISMEMBER/SREM/SMEMBERS.
	ValueKindSet ValueKind = "set"
)

// HashFieldValue represents a field/value pair for hash commands.
type HashFieldValue struct {
	Field string
	Value []byte
}

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

// StringSnapshotEntry is the shutdown-persistence representation for the
// currently supported RDB writer scope: DB 0 string keys only.
type StringSnapshotEntry struct {
	Key       string
	Value     []byte
	ExpiresAt int64
}

// StringSnapshotStats summarizes the export of supported string keys from the
// in-memory store.
type StringSnapshotStats struct {
	TotalKeys              int
	ExportedKeys           int
	SkippedExpiredKeys     int
	SkippedUnsupportedKeys int
}

// SnapshotEntry is a defensive copy of a stored value used for AOF rewrite generation.
type SnapshotEntry struct {
	Key       string
	Kind      ValueKind
	ExpiresAt int64
	String    []byte
	List      [][]byte
	ZSet      []ZSetRangeEntry
	Stream    []StreamEntry
	Hash      []HashFieldValue
	Set       [][]byte
}

// SnapshotStats summarizes a full-value snapshot export.
type SnapshotStats struct {
	TotalKeys          int
	ExportedKeys       int
	SkippedExpiredKeys int
}

// ValueObject is the internal representation of an item stored in RuneDB.
//
// The type is a tagged union: Kind selects which payload field is valid.
// ExpiresAt is stored as a Unix timestamp in milliseconds; 0 means no TTL.
// LastAccessedAt is Phase 11 scaffolding: constructors stamp it at write
// time so LRU-style eviction can consume it later. Read paths intentionally
// do not refresh it here to preserve the RLock fast path on Get.
type ValueObject struct {
	String         []byte
	List           [][]byte
	ZSet           *sortedSet
	Stream         *streamValue
	Hash           map[string][]byte
	Set            map[string]struct{}
	ExpiresAt      int64
	LastAccessedAt int64
	Kind           ValueKind
}

// StringValue returns the string payload for a string value.
func (v *ValueObject) StringValue() ([]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindString {
		return nil, ErrWrongType
	}

	return v.String, nil
}

// ListValue returns the list payload for a list value.
func (v *ValueObject) ListValue() ([][]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindList {
		return nil, ErrWrongType
	}

	return v.List, nil
}

// ZSetValue returns the sorted-set payload for a sorted-set value.
func (v *ValueObject) ZSetValue() (*sortedSet, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSet == nil {
		return nil, errInvalidValueObjectState
	}

	return v.ZSet, nil
}

// StreamValue returns the stream payload for a stream value.
func (v *ValueObject) StreamValue() (*streamValue, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindStream {
		return nil, ErrWrongType
	}
	if v.Stream == nil {
		return nil, errInvalidValueObjectState
	}

	return v.Stream, nil
}

func newStringValue(data []byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		String:         cloneBytes(data),
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindString,
	}
}

// newListValue stores an already-owned list representation without cloning.
// Callers must only pass slices whose element bytes are not aliased with caller-
// controlled memory.
func newListValue(items [][]byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		List:           items,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindList,
	}
}

func newZSetValue(set *sortedSet, expiresAt int64) *ValueObject {
	return &ValueObject{
		ZSet:           set,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindZSet,
	}
}

func newStreamValue(stream *streamValue, expiresAt int64) *ValueObject {
	return &ValueObject{
		Stream:         stream,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindStream,
	}
}

// HashValue returns the hash payload for a hash value.
func (v *ValueObject) HashValue() (map[string][]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, ErrWrongType
	}
	if v.Hash == nil {
		return nil, errInvalidValueObjectState
	}

	return v.Hash, nil
}

// SetValue returns the set payload for a set value.
func (v *ValueObject) SetValue() (map[string]struct{}, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return nil, ErrWrongType
	}
	if v.Set == nil {
		return nil, errInvalidValueObjectState
	}

	return v.Set, nil
}

func newHashValue(fields map[string][]byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		Hash:           fields,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindHash,
	}
}

func newSetValue(members map[string]struct{}, expiresAt int64) *ValueObject {
	return &ValueObject{
		Set:            members,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindSet,
	}
}

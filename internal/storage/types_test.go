package storage

import (
	"errors"
	"testing"
)

func TestValueObjectAccessorsRejectWrongKinds(t *testing.T) {
	value := newStringValue([]byte("hello"), 0)

	if _, err := value.ListValue(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ListValue() error = %v, want ErrWrongType", err)
	}
	if _, err := value.ZSetValue(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZSetValue() error = %v, want ErrWrongType", err)
	}
	if _, err := value.StreamValue(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("StreamValue() error = %v, want ErrWrongType", err)
	}
}

func TestValueObjectAccessorsRejectInvalidTaggedUnionState(t *testing.T) {
	zsetValue := &ValueObject{Kind: ValueKindZSet}
	if _, err := zsetValue.ZSetValue(); !errors.Is(err, errInvalidValueObjectState) {
		t.Fatalf("ZSetValue() error = %v, want errInvalidValueObjectState", err)
	}

	streamValue := &ValueObject{Kind: ValueKindStream}
	if _, err := streamValue.StreamValue(); !errors.Is(err, errInvalidValueObjectState) {
		t.Fatalf("StreamValue() error = %v, want errInvalidValueObjectState", err)
	}

	var nilValue *ValueObject
	if _, err := nilValue.StringValue(); !errors.Is(err, errInvalidValueObjectState) {
		t.Fatalf("nil StringValue() error = %v, want errInvalidValueObjectState", err)
	}
}

func TestValueObjectAccessorsMaterializeCompactValues(t *testing.T) {
	hashValue := newCompactHashValue(newCompactHash([]HashFieldValue{{Field: "f", Value: []byte("v")}}), 0)
	hash, err := hashValue.HashValue()
	if err != nil {
		t.Fatalf("HashValue() error = %v", err)
	}
	if string(hash["f"]) != "v" {
		t.Fatalf("HashValue()[f] = %q, want v", string(hash["f"]))
	}
	hash["f"][0] = 'V'
	got, ok, err := hashValue.hashGet("f")
	if err != nil || !ok || string(got) != "v" {
		t.Fatalf("compact hash after materialized mutation = (%q, %v, %v), want (v, true, nil)", string(got), ok, err)
	}

	zsetValue := newCompactZSetValue(newCompactZSet([]ZSetEntry{{Member: []byte("m"), Score: 1}}), 0)
	zset, err := zsetValue.ZSetValue()
	if err != nil {
		t.Fatalf("ZSetValue() error = %v", err)
	}
	entries := zset.rangeByRank(0, 0)
	if len(entries) != 1 || entries[0].Member != "m" || entries[0].Score != 1 {
		t.Fatalf("ZSetValue().rangeByRank() = %#v, want m/1", entries)
	}
}

func TestStoreRejectsInvalidTaggedUnionState(t *testing.T) {
	store := NewStore()
	store.setValueObjectForTest("leaders", &ValueObject{Kind: ValueKindZSet})
	store.setValueObjectForTest("events", &ValueObject{Kind: ValueKindStream})

	if _, err := store.ZRange("leaders", 0, -1); !errors.Is(err, errInvalidValueObjectState) {
		t.Fatalf("ZRange() error = %v, want errInvalidValueObjectState", err)
	}
	if _, err := store.XRead("events", "0-0"); !errors.Is(err, errInvalidValueObjectState) {
		t.Fatalf("XRead() error = %v, want errInvalidValueObjectState", err)
	}
}

func TestValueObjectStampsLastAccessedAt(t *testing.T) {
	value := newStringValue([]byte("x"), 0)
	if value.LastAccessedAt == 0 {
		t.Fatalf("newStringValue LastAccessedAt = 0, want non-zero")
	}

	list := newListValue([][]byte{[]byte("a")}, 0)
	if list.LastAccessedAt == 0 {
		t.Fatalf("newListValue LastAccessedAt = 0, want non-zero")
	}
}

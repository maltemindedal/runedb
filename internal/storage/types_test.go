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

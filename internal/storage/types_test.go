package storage

import (
	"errors"
	"testing"
)

func TestStoredValueAccessorsRejectWrongKinds(t *testing.T) {
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

func TestStoredValueAccessorsRejectInvalidTaggedUnionState(t *testing.T) {
	zsetValue := StoredValue{Kind: ValueKindZSet}
	if _, err := zsetValue.ZSetValue(); !errors.Is(err, errInvalidStoredValue) {
		t.Fatalf("ZSetValue() error = %v, want errInvalidStoredValue", err)
	}

	streamValue := StoredValue{Kind: ValueKindStream}
	if _, err := streamValue.StreamValue(); !errors.Is(err, errInvalidStoredValue) {
		t.Fatalf("StreamValue() error = %v, want errInvalidStoredValue", err)
	}
}

func TestStoreRejectsInvalidTaggedUnionState(t *testing.T) {
	store := NewStore()
	store.data["leaders"] = StoredValue{Kind: ValueKindZSet}
	store.data["events"] = StoredValue{Kind: ValueKindStream}

	if _, err := store.ZRange("leaders", 0, -1); !errors.Is(err, errInvalidStoredValue) {
		t.Fatalf("ZRange() error = %v, want errInvalidStoredValue", err)
	}
	if _, err := store.XRead("events", "0-0"); !errors.Is(err, errInvalidStoredValue) {
		t.Fatalf("XRead() error = %v, want errInvalidStoredValue", err)
	}
}

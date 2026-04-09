package storage

// ValueKind identifies the logical data shape of a stored value.
type ValueKind string

const (
	// ValueKindString is the Phase 1 string storage type used by SET/GET.
	ValueKindString ValueKind = "string"
)

// StoredValue is the internal representation of an item stored in Godis.
//
// ExpiresAt is stored as a Unix timestamp in milliseconds. A value of 0 means
// the key does not expire.
type StoredValue struct {
	Data      []byte
	ExpiresAt int64
	Kind      ValueKind
}

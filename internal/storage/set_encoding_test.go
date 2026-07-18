package storage

import (
	"strconv"
	"testing"
)

// TestSetUpgradesPastIntSetThreshold verifies a growing integer-only set
// upgrades from the compact sorted-slice encoding to the hashtable once it
// exceeds intSetMaxEntries, so large sets stop paying the O(n) per-member cost.
func TestSetUpgradesPastIntSetThreshold(t *testing.T) {
	total := intSetMaxEntries + 10
	members := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		members = append(members, []byte(strconv.Itoa(i)))
	}

	// Exactly at the threshold: still compact.
	value := newSetValueForMembers(members[:intSetMaxEntries], 0)
	if value.SetEncoding != ValueEncodingCompact {
		t.Fatalf("SetEncoding = %v at threshold, want compact", value.SetEncoding)
	}

	// Adding past the threshold upgrades to the general encoding.
	if _, err := value.setAdd(members[intSetMaxEntries:]); err != nil {
		t.Fatalf("setAdd error = %v", err)
	}
	if value.SetEncoding != ValueEncodingGeneral {
		t.Fatalf("SetEncoding = %v after exceeding threshold, want general", value.SetEncoding)
	}

	got, err := value.setLen()
	if err != nil {
		t.Fatalf("setLen error = %v", err)
	}
	if got != total {
		t.Fatalf("setLen = %d, want %d (no members lost on upgrade)", got, total)
	}
}

// TestLargeIntSetInitialBatchUsesGeneralEncoding verifies an initial SADD larger
// than the threshold skips the compact encoding entirely.
func TestLargeIntSetInitialBatchUsesGeneralEncoding(t *testing.T) {
	total := intSetMaxEntries + 1
	members := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		members = append(members, []byte(strconv.Itoa(i)))
	}

	value := newSetValueForMembers(members, 0)
	if value.SetEncoding != ValueEncodingGeneral {
		t.Fatalf("SetEncoding = %v for oversized initial batch, want general", value.SetEncoding)
	}
}

package rdb

import (
	"bytes"
	"testing"
)

func TestEmptySnapshotReturnsIndependentCopies(t *testing.T) {
	first := EmptySnapshot()
	second := EmptySnapshot()

	if !bytes.Equal(first, second) {
		t.Fatal("EmptySnapshot() payloads differ before mutation")
	}
	if len(first) == 0 {
		t.Fatal("EmptySnapshot() returned empty payload")
	}

	baseline := append([]byte(nil), second...)
	first[0] ^= 0xff

	if !bytes.Equal(second, baseline) {
		t.Fatal("mutating one EmptySnapshot() result changed another result")
	}
	if got := EmptySnapshot(); !bytes.Equal(got, baseline) {
		t.Fatal("subsequent EmptySnapshot() call returned mutated cached payload")
	}
}

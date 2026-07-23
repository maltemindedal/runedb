package aof_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/maltemindedal/stash/internal/aof"
)

func TestWriterPolicyNoFlushesOnAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	writer, err := aof.OpenWriter(context.Background(), path, aof.PolicyNo, nil)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	}()

	payload := []byte("*1\r\n$4\r\nPING\r\n")
	if err := writer.Append(payload); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("file contents = %q, want %q", got, payload)
	}
}

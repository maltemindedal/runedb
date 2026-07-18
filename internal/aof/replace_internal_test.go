package aof

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReplaceFileSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "appendonly.aof")
	temp := filepath.Join(dir, "appendonly.aof.tmp")
	if err := os.WriteFile(temp, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}

	var syncedDir string
	original := syncDir
	syncDir = func(path string) error {
		syncedDir = path
		return original(path)
	}
	defer func() { syncDir = original }()

	if err := replaceFile(temp, target); err != nil {
		t.Fatalf("replaceFile() error = %v", err)
	}
	if syncedDir != target {
		t.Fatalf("syncDir path = %q, want %q", syncedDir, target)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target missing after replace: %v", err)
	}
}

func TestReplaceFilePropagatesSyncError(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "appendonly.aof")
	temp := filepath.Join(dir, "appendonly.aof.tmp")
	if err := os.WriteFile(temp, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}

	sentinel := errors.New("sync failed")
	original := syncDir
	syncDir = func(string) error { return sentinel }
	defer func() { syncDir = original }()

	if err := replaceFile(temp, target); !errors.Is(err, sentinel) {
		t.Fatalf("replaceFile() error = %v, want wrapped %v", err, sentinel)
	}
}

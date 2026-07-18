package rdb

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/storage"
)

func TestReplaceFileSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "dump.rdb")
	temp := filepath.Join(dir, "dump.rdb.tmp")
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
	target := filepath.Join(dir, "dump.rdb")
	temp := filepath.Join(dir, "dump.rdb.tmp")
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

func TestBuildSnapshotRoundTripsThroughLoader(t *testing.T) {
	entries := []storage.StringSnapshotEntry{
		{Key: "name", Value: []byte("RuneDB")},
		{Key: "binary\x00key", Value: []byte{0x00, 0x01, 0x02}, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()},
		{Key: "expired", Value: []byte("gone"), ExpiresAt: time.Now().Add(-time.Second).UnixMilli()},
	}

	payload, stats := BuildSnapshot(entries)
	if len(payload) == 0 {
		t.Fatal("BuildSnapshot() returned empty payload")
	}
	if stats.WrittenKeys != 2 {
		t.Fatalf("stats.WrittenKeys = %d, want 2", stats.WrittenKeys)
	}
	if stats.SkippedExpiredKeys != 1 {
		t.Fatalf("stats.SkippedExpiredKeys = %d, want 1", stats.SkippedExpiredKeys)
	}

	store := storage.NewStore()
	loadStats, err := LoadReader(bytes.NewReader(payload), store)
	if err != nil {
		t.Fatalf("LoadReader() error = %v", err)
	}
	if loadStats.LoadedKeys != 2 {
		t.Fatalf("loadStats.LoadedKeys = %d, want 2", loadStats.LoadedKeys)
	}

	got, ok, err := store.Get("name")
	if err != nil {
		t.Fatalf("Get(name) error = %v", err)
	}
	if !ok || string(got) != "RuneDB" {
		t.Fatalf("Get(name) = (%q, %v), want (%q, true)", string(got), ok, "RuneDB")
	}

	got, ok, err = store.Get("binary\x00key")
	if err != nil {
		t.Fatalf("Get(binary key) error = %v", err)
	}
	if !ok || !bytes.Equal(got, []byte{0x00, 0x01, 0x02}) {
		t.Fatalf("Get(binary key) = (%v, %v), want ([0 1 2], true)", got, ok)
	}

	if _, ok, err := store.Get("expired"); err != nil {
		t.Fatalf("Get(expired) error = %v", err)
	} else if ok {
		t.Fatal("Get(expired) ok = true, want false")
	}
}

func TestSaveSnapshotReplacesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}

	stats, err := SaveSnapshot(path, []storage.StringSnapshotEntry{{Key: "name", Value: []byte("RuneDB")}})
	if err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if stats.WrittenKeys != 1 {
		t.Fatalf("stats.WrittenKeys = %d, want 1", stats.WrittenKeys)
	}

	store := storage.NewStore()
	if _, err := LoadFile(path, store); err != nil {
		t.Fatalf("LoadFile(%q) error = %v", path, err)
	}
	got, ok, err := store.Get("name")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok || string(got) != "RuneDB" {
		t.Fatalf("Get() = (%q, %v), want (%q, true)", string(got), ok, "RuneDB")
	}
}

func TestReplaceFileRetriesAfterExistConflict(t *testing.T) {
	tempPath, targetPath, originalRename, originalRemove := replaceFileTestFixture(t)

	renameCalls := 0
	removeCalls := 0
	renameFile = func(oldpath string, newpath string) error {
		renameCalls++
		if renameCalls == 1 {
			return fs.ErrExist
		}
		return originalRename(oldpath, newpath)
	}
	removeFile = func(name string) error {
		removeCalls++
		return originalRemove(name)
	}

	if err := replaceFile(tempPath, targetPath); err != nil {
		t.Fatalf("replaceFile() error = %v", err)
	}
	if renameCalls != 2 {
		t.Fatalf("rename calls = %d, want 2", renameCalls)
	}
	if removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1", removeCalls)
	}
	assertFileContents(t, targetPath, "new")
}

func TestReplaceFileRetriesAfterWrappedRenameExistConflict(t *testing.T) {
	tempPath, targetPath, originalRename, originalRemove := replaceFileTestFixture(t)

	renameCalls := 0
	removeCalls := 0
	renameFile = func(oldpath string, newpath string) error {
		renameCalls++
		if renameCalls == 1 {
			return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EEXIST}
		}
		return originalRename(oldpath, newpath)
	}
	removeFile = func(name string) error {
		removeCalls++
		return originalRemove(name)
	}

	if err := replaceFile(tempPath, targetPath); err != nil {
		t.Fatalf("replaceFile() error = %v", err)
	}
	if renameCalls != 2 {
		t.Fatalf("rename calls = %d, want 2", renameCalls)
	}
	if removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1", removeCalls)
	}
	assertFileContents(t, targetPath, "new")
}

func TestReplaceFilePreservesExistingFileOnUnexpectedRenameFailure(t *testing.T) {
	tempPath, targetPath, _, originalRemove := replaceFileTestFixture(t)

	boom := errors.New("rename boom")
	removeCalled := false
	renameFile = func(_, _ string) error {
		return boom
	}
	removeFile = func(name string) error {
		removeCalled = true
		return originalRemove(name)
	}

	err := replaceFile(tempPath, targetPath)
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("replaceFile() error = %v, want rename boom", err)
	}
	if removeCalled {
		t.Fatal("replaceFile() removed existing target on unexpected rename failure")
	}
	assertFileContents(t, targetPath, "old")
}

func replaceFileTestFixture(t *testing.T) (string, string, func(string, string) error, func(string) error) {
	t.Helper()

	dir := t.TempDir()
	tempPath := filepath.Join(dir, "temp.rdb")
	targetPath := filepath.Join(dir, "dump.rdb")
	if err := os.WriteFile(tempPath, []byte("new"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", tempPath, err)
	}
	if err := os.WriteFile(targetPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", targetPath, err)
	}

	originalRename := renameFile
	originalRemove := removeFile
	t.Cleanup(func() {
		renameFile = originalRename
		removeFile = originalRemove
	})

	return tempPath, targetPath, originalRename, originalRemove
}

func assertFileContents(t *testing.T, path string, want string) {
	t.Helper()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("target contents = %q, want %q", string(got), want)
	}
}

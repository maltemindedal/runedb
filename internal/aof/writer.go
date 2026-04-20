package aof

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maltemindedal/runedb/internal/storage"
)

var (
	renameFile = os.Rename
	removeFile = os.Remove
)

// Writer appends RESP payloads to an append-only file and can rewrite it in the background.
type Writer struct {
	path   string
	policy Policy
	logger *slog.Logger

	mu             sync.Mutex
	file           *os.File
	writer         *bufio.Writer
	rewriteBuffer  bytes.Buffer
	rewriteActive  bool
	rewritePending bool
	closed         bool
	closeOnce      sync.Once
	closeCh        chan struct{}
	wg             sync.WaitGroup
}

// OpenWriter opens or creates an append-only file writer for the supplied path.
func OpenWriter(ctx context.Context, path string, policy Policy, logger *slog.Logger) (*Writer, error) {
	if path == "" {
		return nil, errors.New("aof: empty file path")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("aof: create directory %q: %w", dir, err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("aof: open %q: %w", path, err)
	}

	writer := &Writer{
		path:    path,
		policy:  policy,
		logger:  logger,
		file:    file,
		writer:  bufio.NewWriter(file),
		closeCh: make(chan struct{}),
	}
	if policy == PolicyEverysec {
		writer.wg.Add(1)
		go writer.runEverysec(ctx)
	}

	return writer, nil
}

// Policy returns the configured appendfsync policy.
func (w *Writer) Policy() Policy {
	if w == nil {
		return PolicyEverysec
	}

	return w.policy
}

// Append writes a payload without forcing an immediate fsync.
func (w *Writer) Append(payload []byte) error {
	return w.append(payload, false)
}

// AppendSync writes a payload and fsyncs it before returning.
func (w *Writer) AppendSync(payload []byte) error {
	return w.append(payload, true)
}

// Close flushes, fsyncs, and closes the underlying AOF file.
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}

	w.closeOnce.Do(func() {
		close(w.closeCh)
	})
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	flushErr := w.syncLocked()
	closeErr := w.file.Close()
	if flushErr != nil && closeErr != nil {
		return errors.Join(flushErr, closeErr)
	}
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return fmt.Errorf("aof: close %q: %w", w.path, closeErr)
	}

	return nil
}

// BeginRewrite starts a background rewrite of the current AOF file.
func (w *Writer) BeginRewrite(ctx context.Context, store *storage.Store) error {
	if w == nil {
		return ErrClosed
	}
	if store == nil {
		return errors.New("aof: store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if w.rewritePending || w.rewriteActive {
		w.mu.Unlock()
		return ErrRewriteInProgress
	}
	w.rewritePending = true
	w.mu.Unlock()

	w.wg.Add(1)
	go w.runRewrite(ctx, store)
	return nil
}

func (w *Writer) append(payload []byte, syncNow bool) error {
	if w == nil || len(payload) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("aof: write %q: %w", w.path, err)
	}
	if w.rewriteActive {
		if _, err := w.rewriteBuffer.Write(payload); err != nil {
			return fmt.Errorf("aof: buffer rewrite payload for %q: %w", w.path, err)
		}
	}
	if syncNow {
		return w.syncLocked()
	}

	return nil
}

func (w *Writer) runEverysec(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sync(); err != nil && !errors.Is(err, ErrClosed) {
				w.logWarn("failed to sync append-only file", "path", w.path, "error", err)
			}
		}
	}
}

func (w *Writer) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	return w.syncLocked()
}

func (w *Writer) syncLocked() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("aof: flush %q: %w", w.path, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("aof: sync %q: %w", w.path, err)
	}

	return nil
}

func (w *Writer) runRewrite(ctx context.Context, store *storage.Store) {
	defer w.wg.Done()

	select {
	case <-w.closeCh:
		w.clearRewriteState(false)
		return
	case <-ctx.Done():
		w.clearRewriteState(false)
		return
	default:
	}

	startedAt := time.Now()
	dir := filepath.Dir(w.path)
	if dir == "" {
		dir = "."
	}
	tempFile, err := os.CreateTemp(dir, filepath.Base(w.path)+".rewrite-*")
	if err != nil {
		w.clearRewriteState(false)
		w.logError("failed to create rewrite temp file", "path", w.path, "error", err)
		return
	}
	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			if removeErr := removeFile(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				w.logWarn("failed to remove rewrite temp file", "path", tempPath, "error", removeErr)
			}
		}
	}()

	activated := false
	entries, snapshotStats := store.SnapshotAllWithWriteBarrier(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.closed {
			w.rewritePending = false
			return
		}

		w.rewriteActive = true
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		activated = true
	})
	if !activated {
		_ = tempFile.Close()
		return
	}

	select {
	case <-w.closeCh:
		w.clearRewriteState(true)
		_ = tempFile.Close()
		return
	case <-ctx.Done():
		w.clearRewriteState(true)
		_ = tempFile.Close()
		return
	default:
	}

	rewriteStats, err := GenerateRewrite(entries, tempFile)
	if err != nil {
		w.clearRewriteState(true)
		_ = tempFile.Close()
		w.logError("failed to generate rewritten append-only file", "path", w.path, "error", err)
		return
	}

	w.mu.Lock()
	if w.closed {
		w.rewriteActive = false
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		w.mu.Unlock()
		_ = tempFile.Close()
		return
	}
	if _, err := tempFile.Write(w.rewriteBuffer.Bytes()); err != nil {
		w.rewriteActive = false
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		w.mu.Unlock()
		_ = tempFile.Close()
		w.logError("failed to write buffered rewrite payload", "path", tempPath, "error", err)
		return
	}
	if err := tempFile.Sync(); err != nil {
		w.rewriteActive = false
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		w.mu.Unlock()
		_ = tempFile.Close()
		w.logError("failed to sync rewritten append-only file", "path", tempPath, "error", err)
		return
	}
	if err := tempFile.Close(); err != nil {
		w.rewriteActive = false
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		w.mu.Unlock()
		w.logError("failed to close rewritten append-only temp file", "path", tempPath, "error", err)
		return
	}
	oldFile := w.file
	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			w.rewriteActive = false
			w.rewritePending = false
			w.rewriteBuffer.Reset()
			w.mu.Unlock()
			w.logError("failed to close current append-only file before rewrite swap", "path", w.path, "error", err)
			return
		}
	}
	if err := replaceFile(tempPath, w.path); err != nil {
		reopened, reopenErr := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if reopenErr == nil {
			w.file = reopened
			w.writer = bufio.NewWriter(reopened)
		}
		w.rewriteActive = false
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		w.mu.Unlock()
		if reopenErr != nil {
			w.logError("failed to replace append-only file with rewrite and reopen original file", "path", w.path, "error", err, "reopen_error", reopenErr)
		} else {
			w.logError("failed to replace append-only file with rewrite", "path", w.path, "error", err)
		}
		return
	}

	newFile, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		w.rewriteActive = false
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		w.mu.Unlock()
		w.logError("failed to reopen append-only file after rewrite", "path", w.path, "error", err)
		return
	}

	w.file = newFile
	w.writer = bufio.NewWriter(newFile)
	w.rewriteActive = false
	w.rewritePending = false
	w.rewriteBuffer.Reset()
	w.mu.Unlock()

	cleanupTemp = false

	w.logInfo(
		"completed append-only file rewrite",
		"path", w.path,
		"snapshot_keys", snapshotStats.ExportedKeys,
		"rewrite_keys", rewriteStats.Keys,
		"rewrite_commands", rewriteStats.Commands,
		"duration", time.Since(startedAt),
	)
}

func (w *Writer) clearRewriteState(active bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if active {
		w.rewriteActive = false
		w.rewriteBuffer.Reset()
	}
	w.rewritePending = false
}

func (w *Writer) logInfo(msg string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Info(msg, args...)
	}
}

func (w *Writer) logWarn(msg string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Warn(msg, args...)
	}
}

func (w *Writer) logError(msg string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Error(msg, args...)
	}
}

func replaceFile(tempPath string, targetPath string) error {
	err := renameFile(tempPath, targetPath)
	if err == nil {
		return nil
	}
	if !isReplaceTargetExistsError(err) {
		return fmt.Errorf("aof: replace %q: %w", targetPath, err)
	}

	removeErr := removeFile(targetPath)
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("aof: replace %q: rename error: %w; remove error: %v", targetPath, err, removeErr)
	}
	if retryErr := renameFile(tempPath, targetPath); retryErr != nil {
		return fmt.Errorf("aof: replace %q after removing existing target: %w", targetPath, retryErr)
	}

	return nil
}

func isReplaceTargetExistsError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, fs.ErrExist) || os.IsExist(err)
}

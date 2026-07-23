package aof

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/maltemindedal/stash/internal/protocol"
)

// LoadFile replays RESP-encoded commands from an append-only file.
func LoadFile(ctx context.Context, path string, replay func(context.Context, protocol.Value) error) (stats LoadStats, err error) {
	if replay == nil {
		return LoadStats{}, errors.New("aof: nil replay function")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	file, err := os.Open(path)
	if err != nil {
		return LoadStats{}, fmt.Errorf("aof: read %q: %w", path, err)
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("aof: close %q: %w", path, closeErr)
		}
	}()

	parser := protocol.NewParser(bufio.NewReader(file))
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stats, fmt.Errorf("aof: load %q canceled: %w", path, ctxErr)
		}

		value, parseErr := parser.Parse()
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				return stats, nil
			}
			if isRecoverableTruncation(parseErr) {
				stats.TruncatedTail = true
				return stats, nil
			}

			return stats, fmt.Errorf("aof: parse %q after %d commands: %w", path, stats.ReplayedCommands, parseErr)
		}
		if replayErr := replay(ctx, value); replayErr != nil {
			return stats, fmt.Errorf("aof: replay command %d from %q: %w", stats.ReplayedCommands+1, path, replayErr)
		}
		stats.ReplayedCommands++
	}
}

func isRecoverableTruncation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// A frame whose final CRLF terminator never arrived is a torn trailing write,
	// not corruption. The parser reports it with the typed ErrMissingCRLF
	// sentinel, so match on that rather than the error message text.
	return errors.Is(err, protocol.ErrMissingCRLF)
}

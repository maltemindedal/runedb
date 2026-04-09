package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New creates a structured slog logger with the requested level.
func New(level string) *slog.Logger {
	options := &slog.HandlerOptions{Level: parseLevel(level)}
	return slog.New(slog.NewTextHandler(os.Stdout, options))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

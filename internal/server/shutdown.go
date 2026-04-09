package server

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// NotifyContext returns a context cancelled by SIGINT or SIGTERM.
func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

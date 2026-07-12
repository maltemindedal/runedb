//go:build !linux && !darwin

package server

import (
	"context"
	"net"
)

// serveEventLoop reports that OS I/O multiplexing is not supported on this
// platform. The caller falls back to the goroutine-per-connection path.
func (s *Server) serveEventLoop(context.Context, net.Listener) error {
	return errEventLoopUnsupported
}

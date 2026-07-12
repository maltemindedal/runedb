//go:build linux || darwin

package server

import (
	"net"
	"sync"
	"syscall"
)

// wakePipe is the self-wakeup half shared by the platform pollers: a
// non-blocking pipe whose read end sits in the poll set so other goroutines
// can unblock a Wait call by writing a byte. Construction is platform-specific
// (Pipe2 on Linux, Pipe plus SetNonblock on macOS); the drain, wake, and close
// protocol is identical and lives here so fixes reach both pollers.
type wakePipe struct {
	readFD  int
	writeFD int

	// mu serializes wake against close so a late cross-goroutine wakeup
	// cannot write to a closed (and potentially reused) descriptor.
	mu     sync.Mutex
	closed bool
}

// drain empties the pipe after its read end reported readable. Owned by the
// poller goroutine.
func (p *wakePipe) drain() {
	var buf [64]byte
	for {
		n, err := syscall.Read(p.readFD, buf[:])
		if n <= 0 || err != nil {
			return
		}
	}
}

// wake unblocks a concurrent Wait call. Safe for concurrent use.
func (p *wakePipe) wake() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return net.ErrClosed
	}
	for {
		_, err := syscall.Write(p.writeFD, []byte{1})
		switch err {
		case syscall.EINTR:
			continue
		case syscall.EAGAIN:
			// The pipe is full, so a wakeup is already pending.
			return nil
		default:
			return err
		}
	}
}

// close releases both pipe descriptors and fails later wake calls.
func (p *wakePipe) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	closeIgnoringError(p.writeFD)
	closeIgnoringError(p.readFD)
}

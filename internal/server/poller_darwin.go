//go:build darwin

package server

import (
	"fmt"
	"net"
	"sync"
	"syscall"
)

// newPoller constructs the macOS readiness poller backed by kqueue. A
// non-blocking pipe registered for EVFILT_READ provides cross-goroutine
// wakeups for Wake.
func newPoller() (poller, error) {
	kq, err := syscall.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue: %w", err)
	}
	syscall.CloseOnExec(kq)

	var pipeFDs [2]int
	if err := syscall.Pipe(pipeFDs[:]); err != nil {
		closeIgnoringError(kq)
		return nil, fmt.Errorf("wake pipe: %w", err)
	}
	for _, fd := range pipeFDs {
		if err := syscall.SetNonblock(fd, true); err != nil {
			closeIgnoringError(kq)
			closeIgnoringError(pipeFDs[0])
			closeIgnoringError(pipeFDs[1])
			return nil, fmt.Errorf("wake pipe nonblock: %w", err)
		}
		syscall.CloseOnExec(fd)
	}

	p := &kqueuePoller{
		kq:       kq,
		wakeR:    pipeFDs[0],
		wakeW:    pipeFDs[1],
		kqEvents: make([]syscall.Kevent_t, pollerEventBatch),
	}
	if err := p.change(p.wakeR, syscall.EVFILT_READ, syscall.EV_ADD); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("register wake pipe: %w", err)
	}

	return p, nil
}

type kqueuePoller struct {
	kq    int
	wakeR int
	wakeW int

	kqEvents []syscall.Kevent_t

	// wakeMu serializes Wake against Close so a late cross-goroutine wakeup
	// cannot write to a closed (and potentially reused) descriptor.
	wakeMu     sync.Mutex
	wakeClosed bool
}

func (p *kqueuePoller) change(fd int, filter int16, flags uint16) error {
	changes := []syscall.Kevent_t{{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  flags,
	}}
	_, err := syscall.Kevent(p.kq, changes, nil, nil)
	return err
}

func (p *kqueuePoller) Add(fd int) error {
	return p.change(fd, syscall.EVFILT_READ, syscall.EV_ADD)
}

func (p *kqueuePoller) SetWrite(fd int, writable bool) error {
	if writable {
		return p.change(fd, syscall.EVFILT_WRITE, syscall.EV_ADD)
	}

	err := p.change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
	if err == syscall.ENOENT {
		return nil
	}
	return err
}

func (p *kqueuePoller) Remove(fd int) error {
	readErr := p.change(fd, syscall.EVFILT_READ, syscall.EV_DELETE)
	if readErr == syscall.ENOENT {
		readErr = nil
	}
	writeErr := p.change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
	if writeErr == syscall.ENOENT {
		writeErr = nil
	}
	if readErr != nil {
		return readErr
	}
	return writeErr
}

func (p *kqueuePoller) Wait(events []pollEvent) (int, error) {
	for {
		n, err := syscall.Kevent(p.kq, nil, p.kqEvents, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("kevent: %w", err)
		}

		out := 0
		for _, kqEvent := range p.kqEvents[:n] {
			fd := int(kqEvent.Ident)
			if fd == p.wakeR {
				p.drainWake()
				continue
			}
			if out >= len(events) {
				break
			}
			events[out] = pollEvent{
				fd:       fd,
				readable: kqEvent.Filter == syscall.EVFILT_READ || kqEvent.Flags&syscall.EV_ERROR != 0,
				writable: kqEvent.Filter == syscall.EVFILT_WRITE,
			}
			out++
		}

		return out, nil
	}
}

func (p *kqueuePoller) drainWake() {
	var buf [64]byte
	for {
		n, err := syscall.Read(p.wakeR, buf[:])
		if n <= 0 || err != nil {
			return
		}
	}
}

func (p *kqueuePoller) Wake() error {
	p.wakeMu.Lock()
	defer p.wakeMu.Unlock()

	if p.wakeClosed {
		return net.ErrClosed
	}
	for {
		_, err := syscall.Write(p.wakeW, []byte{1})
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

func (p *kqueuePoller) Close() error {
	p.wakeMu.Lock()
	p.wakeClosed = true
	closeIgnoringError(p.wakeW)
	closeIgnoringError(p.wakeR)
	p.wakeMu.Unlock()

	return syscall.Close(p.kq)
}

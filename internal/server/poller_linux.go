//go:build linux

package server

import (
	"fmt"
	"net"
	"sync"
	"syscall"
)

// newPoller constructs the Linux readiness poller backed by epoll. A
// non-blocking pipe registered in the epoll set provides cross-goroutine
// wakeups for Wake.
func newPoller() (poller, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}

	var pipeFDs [2]int
	if err := syscall.Pipe2(pipeFDs[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		closeIgnoringError(epfd)
		return nil, fmt.Errorf("wake pipe: %w", err)
	}

	p := &epollPoller{
		epfd:     epfd,
		wakeR:    pipeFDs[0],
		wakeW:    pipeFDs[1],
		epEvents: make([]syscall.EpollEvent, pollerEventBatch),
	}
	wakeEvent := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(p.wakeR)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, p.wakeR, &wakeEvent); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("register wake pipe: %w", err)
	}

	return p, nil
}

type epollPoller struct {
	epfd  int
	wakeR int
	wakeW int

	epEvents []syscall.EpollEvent

	// wakeMu serializes Wake against Close so a late cross-goroutine wakeup
	// cannot write to a closed (and potentially reused) descriptor.
	wakeMu     sync.Mutex
	wakeClosed bool
}

func (p *epollPoller) Add(fd int) error {
	event := syscall.EpollEvent{Events: epollReadEvents, Fd: int32(fd)}
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_ADD, fd, &event)
}

func (p *epollPoller) SetWrite(fd int, writable bool) error {
	events := uint32(epollReadEvents)
	if writable {
		events |= syscall.EPOLLOUT
	}
	event := syscall.EpollEvent{Events: events, Fd: int32(fd)}
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_MOD, fd, &event)
}

func (p *epollPoller) Remove(fd int) error {
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
}

const epollReadEvents = syscall.EPOLLIN | syscall.EPOLLRDHUP

func (p *epollPoller) Wait(events []pollEvent) (int, error) {
	for {
		n, err := syscall.EpollWait(p.epfd, p.epEvents, -1)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("epoll_wait: %w", err)
		}

		out := 0
		for _, epEvent := range p.epEvents[:n] {
			fd := int(epEvent.Fd)
			if fd == p.wakeR {
				p.drainWake()
				continue
			}
			if out >= len(events) {
				break
			}
			events[out] = pollEvent{
				fd:       fd,
				readable: epEvent.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLHUP|syscall.EPOLLERR) != 0,
				writable: epEvent.Events&syscall.EPOLLOUT != 0,
			}
			out++
		}

		return out, nil
	}
}

func (p *epollPoller) drainWake() {
	var buf [64]byte
	for {
		n, err := syscall.Read(p.wakeR, buf[:])
		if n <= 0 || err != nil {
			return
		}
	}
}

func (p *epollPoller) Wake() error {
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

func (p *epollPoller) Close() error {
	p.wakeMu.Lock()
	p.wakeClosed = true
	closeIgnoringError(p.wakeW)
	closeIgnoringError(p.wakeR)
	p.wakeMu.Unlock()

	return syscall.Close(p.epfd)
}

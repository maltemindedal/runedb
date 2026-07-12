//go:build linux || darwin

package server

import (
	"syscall"
	"testing"
	"time"
)

func testSocketPair(t *testing.T) (int, int) {
	t.Helper()

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("Socketpair() error = %v", err)
	}
	for _, fd := range fds {
		if err := syscall.SetNonblock(fd, true); err != nil {
			t.Fatalf("SetNonblock(%d) error = %v", fd, err)
		}
	}
	t.Cleanup(func() {
		closeIgnoringError(fds[0])
		closeIgnoringError(fds[1])
	})

	return fds[0], fds[1]
}

func waitForEvents(t *testing.T, p poller) []pollEvent {
	t.Helper()

	events := make([]pollEvent, pollerEventBatch)
	n, err := p.Wait(events)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return events[:n]
}

func TestPollerReportsReadReadiness(t *testing.T) {
	p, err := newPoller()
	if err != nil {
		t.Fatalf("newPoller() error = %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	local, remote := testSocketPair(t)
	if err := p.Add(local); err != nil {
		t.Fatalf("Add(%d) error = %v", local, err)
	}

	if _, err := syscall.Write(remote, []byte("x")); err != nil {
		t.Fatalf("Write(remote) error = %v", err)
	}

	events := waitForEvents(t, p)
	if len(events) != 1 || events[0].fd != local || !events[0].readable {
		t.Fatalf("events = %#v, want one readable event for fd %d", events, local)
	}
}

func TestPollerTogglesWriteInterest(t *testing.T) {
	p, err := newPoller()
	if err != nil {
		t.Fatalf("newPoller() error = %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	local, _ := testSocketPair(t)
	if err := p.Add(local); err != nil {
		t.Fatalf("Add(%d) error = %v", local, err)
	}

	if err := p.Set(local, true, true); err != nil {
		t.Fatalf("Set(read+write) error = %v", err)
	}
	events := waitForEvents(t, p)
	writable := false
	for _, event := range events {
		if event.fd == local && event.writable {
			writable = true
		}
	}
	if !writable {
		t.Fatalf("events = %#v, want writable event for fd %d", events, local)
	}

	if err := p.Set(local, true, false); err != nil {
		t.Fatalf("Set(read only) error = %v", err)
	}
	// Disabling twice must stay idempotent even when no write filter exists.
	if err := p.Set(local, true, false); err != nil {
		t.Fatalf("Set(read only) second call error = %v", err)
	}
}

func TestPollerWakeUnblocksWait(t *testing.T) {
	p, err := newPoller()
	if err != nil {
		t.Fatalf("newPoller() error = %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		events := make([]pollEvent, pollerEventBatch)
		if _, err := p.Wait(events); err != nil {
			t.Errorf("Wait() error = %v", err)
		}
	}()

	time.Sleep(10 * time.Millisecond)
	if err := p.Wake(); err != nil {
		t.Fatalf("Wake() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wake() did not unblock Wait() within timeout")
	}
}

func TestPollerRemoveStopsReporting(t *testing.T) {
	p, err := newPoller()
	if err != nil {
		t.Fatalf("newPoller() error = %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	local, remote := testSocketPair(t)
	if err := p.Add(local); err != nil {
		t.Fatalf("Add(%d) error = %v", local, err)
	}
	if err := p.Remove(local); err != nil {
		t.Fatalf("Remove(%d) error = %v", local, err)
	}

	if _, err := syscall.Write(remote, []byte("x")); err != nil {
		t.Fatalf("Write(remote) error = %v", err)
	}

	done := make(chan []pollEvent, 1)
	go func() {
		events := make([]pollEvent, pollerEventBatch)
		n, err := p.Wait(events)
		if err != nil {
			t.Errorf("Wait() error = %v", err)
		}
		done <- events[:n]
	}()

	time.Sleep(20 * time.Millisecond)
	if err := p.Wake(); err != nil {
		t.Fatalf("Wake() error = %v", err)
	}

	select {
	case events := <-done:
		for _, event := range events {
			if event.fd == local {
				t.Fatalf("removed fd %d still reported: %#v", local, event)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() did not return within timeout")
	}
}

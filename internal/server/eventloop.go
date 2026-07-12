//go:build linux || darwin

package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/maltemindedal/runedb/internal/protocol"
)

// pollerEventBatch bounds how many readiness events one Wait call returns.
const pollerEventBatch = 128

// eventLoopReadChunk is the size of the shared buffer used for socket reads.
const eventLoopReadChunk = 64 * 1024

// eventLoopReadBudget caps how many bytes one readiness event may read from a
// single connection before yielding, so one fast writer cannot starve the
// other connections. Level-triggered readiness re-reports leftover data.
const eventLoopReadBudget = 1 << 20

// pollEvent is one readiness notification translated from the OS poller.
type pollEvent struct {
	fd       int
	readable bool
	writable bool
}

// poller abstracts the OS readiness-notification facility (epoll on Linux,
// kqueue on macOS). Wait and the mutation methods are owned by the event-loop
// goroutine; only Wake is safe to call from other goroutines.
type poller interface {
	// Add registers fd for read readiness.
	Add(fd int) error
	// SetWrite enables or disables write-readiness reporting for fd.
	SetWrite(fd int, writable bool) error
	// Remove deregisters fd entirely.
	Remove(fd int) error
	// Wait blocks until readiness events arrive and translates them into
	// events, returning how many entries were filled.
	Wait(events []pollEvent) (int, error)
	// Wake unblocks a concurrent Wait call.
	Wake() error
	// Close releases the poller resources.
	Close() error
}

// errEventLoopWouldBlock reports that a socket write consumed only part of the
// pending output because the kernel send buffer is full.
var errEventLoopWouldBlock = errors.New("server: socket write would block")

// serveEventLoop serves every client connection through a single goroutine
// driven by OS readiness notifications, dispatching readable and writable
// sockets through per-connection ConnMachine state machines.
func (s *Server) serveEventLoop(ctx context.Context, listener net.Listener) error {
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		return fmt.Errorf("server: event loop requires a TCP listener, got %T", listener)
	}

	// Duplicate the listener socket so the event loop owns a raw descriptor.
	// The original listener stays bound; both descriptors share one accept
	// queue and shutdown closing the original does not disturb the duplicate.
	listenerFile, err := tcpListener.File()
	if err != nil {
		return fmt.Errorf("server: dup listener descriptor: %w", err)
	}
	listenFD := int(listenerFile.Fd())
	if err := syscall.SetNonblock(listenFD, true); err != nil {
		_ = listenerFile.Close()
		return fmt.Errorf("server: set listener nonblocking: %w", err)
	}

	p, err := newPoller()
	if err != nil {
		_ = listenerFile.Close()
		return fmt.Errorf("server: create poller: %w", err)
	}
	if err := p.Add(listenFD); err != nil {
		_ = p.Close()
		_ = listenerFile.Close()
		return fmt.Errorf("server: register listener: %w", err)
	}

	loop := &eventLoop{
		srv:          s,
		poller:       p,
		listenerFile: listenerFile,
		listenFD:     listenFD,
		localAddr:    listener.Addr(),
		conns:        make(map[int]*eventConn),
		events:       make([]pollEvent, pollerEventBatch),
		readBuf:      make([]byte, eventLoopReadChunk),
	}

	s.logger.Info("serving clients through the event loop", "poller", "os-readiness")
	return loop.run(ctx)
}

// connCommandRunner returns a ConnCommandRunner that executes one parsed
// request through the same pipeline as handleConnection: monitor observation,
// command execution, durability preparation, replica registration, and
// mutation-effect finalization. Responses are returned for the ConnMachine to
// buffer rather than written directly, so mutation effects finalize before the
// reply reaches the socket instead of after, as on the goroutine path. A
// returned error is fatal for the connection.
func (s *Server) connCommandRunner(clientID uint64, conn net.Conn, logger *slog.Logger) ConnCommandRunner {
	return func(ctx context.Context, request protocol.Value) ([]protocol.Value, error) {
		if s.monitorRegistry.HasSubscribers() {
			s.broadcastMonitorEvent(observeCommand(request, clientID, conn))
		}

		result, execErr := s.executor.ExecuteDetailed(ctx, request)
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return nil, execErr
			}
			logger.Debug("command execution failed", "error", execErr)
			result = SingleResponse(responseError(execErr))
		}

		durabilityPayload, durableErr := s.prepareDurabilityBeforeResponse(result.Durability, logger)
		if durableErr != nil {
			result = SingleResponse(persistenceFailureResponse())
			result.Propagation = nil
			result.Durability = nil
			durabilityPayload = nil
		}

		if result.RegisterReplica {
			s.registerReplicaPeer(clientID, conn)
		}
		s.finalizeMutationEffects(ctx, result.Durability, durabilityPayload, result.Propagation, logger)
		s.commandsProcessed.Add(1)

		return result.Responses, nil
	}
}

// eventLoop drives all client connections from one goroutine. Connection state
// (ConnMachine, interest flags) is owned exclusively by that goroutine; the
// mutex only guards the queues that other goroutines use to hand work to the
// loop (async push frames and close requests), which are paired with a poller
// wakeup.
type eventLoop struct {
	srv          *Server
	poller       poller
	listenerFile *os.File
	listenFD     int
	localAddr    net.Addr
	ctx          context.Context

	conns   map[int]*eventConn
	events  []pollEvent
	readBuf []byte

	mu        sync.Mutex
	pushed    []*eventConn
	closeReqs []*eventConn
}

// eventConn tracks one accepted connection inside the event loop. All fields
// are owned by the loop goroutine except pushBuf, pushQueued, closeRequested,
// and detached, which are guarded by eventLoop.mu.
type eventConn struct {
	fd         int
	clientID   uint64
	remoteAddr net.Addr
	state      *ClientState
	machine    *ConnMachine
	ctx        context.Context
	run        ConnCommandRunner
	handle     *eventConnHandle
	logger     *slog.Logger
	wantWrite  bool

	pushBuf        []byte
	pushQueued     bool
	closeRequested bool
	detached       bool
}

func (l *eventLoop) run(ctx context.Context) error {
	l.ctx = ctx
	defer l.cleanup()

	stopWake := context.AfterFunc(ctx, func() {
		_ = l.poller.Wake()
	})
	defer stopWake()

	for {
		n, err := l.poller.Wait(l.events)
		if err != nil {
			return fmt.Errorf("server: event loop wait: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}

		l.applyQueuedWork()

		for _, event := range l.events[:n] {
			if event.fd == l.listenFD {
				if event.readable {
					l.acceptReady()
				}
				continue
			}

			conn := l.conns[event.fd]
			if conn == nil {
				continue
			}
			if event.readable {
				l.connReadable(conn)
			}
			// The readable path may have closed and unregistered the
			// connection; re-check before flushing.
			if event.writable && l.conns[event.fd] == conn {
				l.flushConn(conn)
			}
		}
	}
}

// applyQueuedWork moves cross-goroutine push frames into connection write
// buffers and applies queued close requests.
func (l *eventLoop) applyQueuedWork() {
	type pushWork struct {
		conn *eventConn
		data []byte
	}

	l.mu.Lock()
	pushes := make([]pushWork, 0, len(l.pushed))
	for _, conn := range l.pushed {
		if conn.detached || len(conn.pushBuf) == 0 {
			conn.pushQueued = false
			continue
		}
		pushes = append(pushes, pushWork{conn: conn, data: conn.pushBuf})
		conn.pushBuf = nil
		conn.pushQueued = false
	}
	l.pushed = l.pushed[:0]
	closes := append([]*eventConn(nil), l.closeReqs...)
	l.closeReqs = l.closeReqs[:0]
	l.mu.Unlock()

	for _, push := range pushes {
		if l.conns[push.conn.fd] != push.conn {
			continue
		}
		if err := push.conn.machine.BufferEncoded(push.data); err != nil {
			l.closeConn(push.conn, err)
			continue
		}
		l.flushConn(push.conn)
	}

	for _, conn := range closes {
		if l.conns[conn.fd] == conn {
			l.closeConn(conn, nil)
		}
	}
}

// queuePush appends an async push frame (pub/sub message, monitor event, or
// replication payload) for delivery by the loop goroutine. Safe for concurrent
// use; the per-client responseMu already serializes whole frames.
func (l *eventLoop) queuePush(conn *eventConn, payload []byte) (int, error) {
	l.mu.Lock()
	if conn.detached {
		l.mu.Unlock()
		return 0, net.ErrClosed
	}
	conn.pushBuf = append(conn.pushBuf, payload...)
	alreadyQueued := conn.pushQueued
	conn.pushQueued = true
	if !alreadyQueued {
		l.pushed = append(l.pushed, conn)
	}
	l.mu.Unlock()

	if err := l.poller.Wake(); err != nil {
		return 0, err
	}
	return len(payload), nil
}

// requestClose asks the loop goroutine to close conn. Safe for concurrent use.
func (l *eventLoop) requestClose(conn *eventConn) {
	l.mu.Lock()
	if conn.detached || conn.closeRequested {
		l.mu.Unlock()
		return
	}
	conn.closeRequested = true
	l.closeReqs = append(l.closeReqs, conn)
	l.mu.Unlock()

	_ = l.poller.Wake()
}

func (l *eventLoop) acceptReady() {
	for {
		fd, sa, err := syscall.Accept(l.listenFD)
		if err != nil {
			switch err {
			case syscall.EAGAIN:
				return
			case syscall.EINTR, syscall.ECONNABORTED:
				continue
			default:
				l.srv.logger.Warn("event loop accept failed", "error", err)
				return
			}
		}

		syscall.CloseOnExec(fd)
		if err := syscall.SetNonblock(fd, true); err != nil {
			l.srv.logger.Warn("failed to set accepted connection nonblocking", "error", err)
			closeIgnoringError(fd)
			continue
		}
		// Match the latency behavior of the net package, which disables
		// Nagle's algorithm on accepted TCP connections.
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); err != nil {
			l.srv.logger.Debug("failed to set TCP_NODELAY on accepted connection", "error", err)
		}

		l.registerConn(fd, sockaddrTCPAddr(sa))
	}
}

func (l *eventLoop) registerConn(fd int, remoteAddr net.Addr) {
	conn := &eventConn{fd: fd, remoteAddr: remoteAddr}
	conn.handle = &eventConnHandle{loop: l, conn: conn}

	conn.clientID = l.srv.registry.Add(conn.handle)
	conn.state = l.srv.createClientState(conn.clientID)
	conn.state.BindResponseWriter(bufio.NewWriter(&eventConnPushWriter{loop: l, conn: conn}))

	remoteAddrText := ""
	if remoteAddr != nil {
		remoteAddrText = remoteAddr.String()
	}
	conn.state.SetRemoteAddr(remoteAddrText)

	conn.machine = NewConnMachine(conn.state)
	conn.ctx = WithClientState(l.ctx, conn.state)
	conn.logger = l.srv.logger.With("client_id", conn.clientID, "remote_addr", remoteAddrText)
	conn.run = l.srv.connCommandRunner(conn.clientID, conn.handle, conn.logger)

	if err := l.poller.Add(fd); err != nil {
		conn.logger.Warn("failed to register accepted connection with poller", "error", err)
		conn.machine.Close(err)
		l.srv.registry.Remove(conn.clientID)
		l.srv.removeClientState(conn.clientID)
		closeIgnoringError(fd)
		return
	}

	l.conns[fd] = conn
	conn.logger.Debug("client connected")
}

func (l *eventLoop) connReadable(conn *eventConn) {
	budget := eventLoopReadBudget
	sawEOF := false

	for budget > 0 {
		n, err := syscall.Read(conn.fd, l.readBuf)
		if n > 0 {
			budget -= n
			if conn.machine.State() == ConnStateActive {
				if feedErr := conn.machine.Feed(l.readBuf[:n]); feedErr != nil {
					l.closeConn(conn, feedErr)
					return
				}
			}
			// A machine in the closing state drains its buffered replies but
			// rejects new input, so bytes read past that point are discarded.
		}
		if err != nil {
			if err == syscall.EAGAIN {
				break
			}
			if err == syscall.EINTR {
				continue
			}
			l.closeConn(conn, err)
			return
		}
		if n == 0 {
			sawEOF = true
			break
		}
		if n < len(l.readBuf) {
			break
		}
	}

	l.processConn(conn)

	if sawEOF && l.conns[conn.fd] == conn {
		// The peer finished sending; already-parsed requests were executed and
		// flushed above, matching the goroutine path which serves the pipeline
		// tail before observing EOF.
		l.closeConn(conn, nil)
	}
}

// processConn executes parsed requests and flushes buffered output.
func (l *eventLoop) processConn(conn *eventConn) {
	if conn.machine.State() != ConnStateClosed {
		if err := conn.machine.ProcessPending(conn.ctx, conn.run); err != nil {
			l.closeConn(conn, err)
			return
		}
	}
	l.flushConn(conn)
}

// flushConn writes pending output, tracks write interest for backpressure, and
// finishes connections whose machine reached the closed state.
func (l *eventLoop) flushConn(conn *eventConn) {
	if conn.machine.State() == ConnStateClosed {
		l.closeConn(conn, conn.machine.Err())
		return
	}

	if err := conn.machine.Flush(fdWriter{fd: conn.fd}); err != nil && !errors.Is(err, errEventLoopWouldBlock) {
		l.closeConn(conn, err)
		return
	}
	if conn.machine.State() == ConnStateClosed {
		l.closeConn(conn, conn.machine.Err())
		return
	}

	l.setWriteInterest(conn, conn.machine.HasPendingOutput())
}

func (l *eventLoop) setWriteInterest(conn *eventConn, want bool) {
	if conn.wantWrite == want {
		return
	}
	if err := l.poller.SetWrite(conn.fd, want); err != nil {
		l.closeConn(conn, err)
		return
	}
	conn.wantWrite = want
}

func (l *eventLoop) closeConn(conn *eventConn, cause error) {
	if l.conns[conn.fd] != conn {
		return
	}
	delete(l.conns, conn.fd)

	l.mu.Lock()
	conn.detached = true
	conn.pushBuf = nil
	l.mu.Unlock()

	conn.machine.Close(cause)
	if err := l.poller.Remove(conn.fd); err != nil {
		conn.logger.Debug("failed to deregister connection from poller", "error", err)
	}
	closeIgnoringError(conn.fd)

	if peer := l.srv.replicaPeers.Remove(conn.clientID); peer != nil {
		conn.logger.Info("replica disconnected", "replica_id", conn.clientID, "listening_port", peer.ListeningPort)
	}
	l.srv.registry.Remove(conn.clientID)
	l.srv.removeClientState(conn.clientID)
	conn.logger.Debug("client disconnected")
}

func (l *eventLoop) cleanup() {
	conns := make([]*eventConn, 0, len(l.conns))
	for _, conn := range l.conns {
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		l.closeConn(conn, nil)
	}

	if err := l.poller.Close(); err != nil {
		l.srv.logger.Debug("failed to close poller", "error", err)
	}
	if err := l.listenerFile.Close(); err != nil {
		l.srv.logger.Debug("failed to close event loop listener descriptor", "error", err)
	}
}

// fdWriter adapts a non-blocking socket descriptor to io.Writer for
// ConnMachine.Flush. A full kernel send buffer surfaces as
// errEventLoopWouldBlock with a partial byte count, leaving the remainder
// buffered in the machine until the poller reports the socket writable.
type fdWriter struct {
	fd int
}

func (w fdWriter) Write(p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := syscall.Write(w.fd, p[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			if err == syscall.EAGAIN {
				return total, errEventLoopWouldBlock
			}
			return total, err
		}
	}
	return total, nil
}

// eventConnPushWriter is the io.Writer bound (through a bufio.Writer) as the
// connection's ClientState response writer. Async deliveries from other
// goroutines land here and are handed to the loop goroutine for ordered
// flushing.
type eventConnPushWriter struct {
	loop *eventLoop
	conn *eventConn
}

func (w *eventConnPushWriter) Write(p []byte) (int, error) {
	return w.loop.queuePush(w.conn, p)
}

// eventConnHandle is the minimal net.Conn surface registered with the client
// registry so shutdown, INFO connected-client counts, and replica bookkeeping
// keep working for event-loop connections. Close hands the actual teardown to
// the loop goroutine; direct reads and writes are not supported because all
// socket I/O is owned by the loop.
type eventConnHandle struct {
	loop *eventLoop
	conn *eventConn
}

var errEventConnHandleIO = errors.New("server: event loop connections do not support direct I/O")

func (h *eventConnHandle) Read([]byte) (int, error)  { return 0, errEventConnHandleIO }
func (h *eventConnHandle) Write([]byte) (int, error) { return 0, errEventConnHandleIO }

func (h *eventConnHandle) Close() error {
	h.loop.requestClose(h.conn)
	return nil
}

func (h *eventConnHandle) LocalAddr() net.Addr  { return h.loop.localAddr }
func (h *eventConnHandle) RemoteAddr() net.Addr { return h.conn.remoteAddr }

func (h *eventConnHandle) SetDeadline(time.Time) error      { return nil }
func (h *eventConnHandle) SetReadDeadline(time.Time) error  { return nil }
func (h *eventConnHandle) SetWriteDeadline(time.Time) error { return nil }

func sockaddrTCPAddr(sa syscall.Sockaddr) net.Addr {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		ip := make(net.IP, len(sa.Addr))
		copy(ip, sa.Addr[:])
		return &net.TCPAddr{IP: ip, Port: sa.Port}
	case *syscall.SockaddrInet6:
		ip := make(net.IP, len(sa.Addr))
		copy(ip, sa.Addr[:])
		return &net.TCPAddr{IP: ip, Port: sa.Port}
	default:
		return nil
	}
}

func closeIgnoringError(fd int) {
	_ = syscall.Close(fd)
}

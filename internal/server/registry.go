package server

import (
	"errors"
	"net"
	"sync"
)

// ClientConn is the connection surface the server tracks per client: enough
// to close it and attribute it. Narrowing this from net.Conn keeps callers
// from depending on direct socket I/O or deadlines, which the event-loop
// networking mode does not expose — its connections are driven exclusively by
// the loop goroutine.
type ClientConn interface {
	Close() error
	RemoteAddr() net.Addr
}

// Registry tracks active client connections.
type Registry struct {
	mu      sync.RWMutex
	nextID  uint64
	clients map[uint64]ClientConn
}

// NewRegistry creates an empty client registry.
func NewRegistry() *Registry {
	return &Registry{clients: make(map[uint64]ClientConn)}
}

// Add registers a new client connection and returns its unique ID.
func (r *Registry) Add(conn ClientConn) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	r.clients[r.nextID] = conn
	return r.nextID
}

// Remove removes a client connection from the registry.
func (r *Registry) Remove(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.clients, id)
}

// Count returns the number of active client connections.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.clients)
}

// CloseAll closes every tracked client connection.
func (r *Registry) CloseAll() error {
	r.mu.Lock()
	clients := make([]ClientConn, 0, len(r.clients))
	for id, conn := range r.clients {
		clients = append(clients, conn)
		delete(r.clients, id)
	}
	r.mu.Unlock()

	var closeErr error
	for _, conn := range clients {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}

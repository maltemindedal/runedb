package server

import (
	"net"
	"sync"
)

// Registry tracks active client connections.
type Registry struct {
	mu      sync.RWMutex
	nextID  uint64
	clients map[uint64]net.Conn
}

// NewRegistry creates an empty client registry.
func NewRegistry() *Registry {
	return &Registry{clients: make(map[uint64]net.Conn)}
}

// Add registers a new client connection and returns its unique ID.
func (r *Registry) Add(conn net.Conn) uint64 {
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
func (r *Registry) CloseAll() {
	r.mu.Lock()
	clients := make([]net.Conn, 0, len(r.clients))
	for id, conn := range r.clients {
		clients = append(clients, conn)
		delete(r.clients, id)
	}
	r.mu.Unlock()

	for _, conn := range clients {
		_ = conn.Close()
	}
}

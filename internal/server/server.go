package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/maltemindedal/runedb/internal/config"
	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/storage"
)

type executor interface {
	Execute(context.Context, protocol.Value) (protocol.Value, error)
}

type watchRegistryProvider interface {
	WatchRegistry() *WatchRegistry
}

// Server owns the TCP listener, active clients, and command execution pipeline.
type Server struct {
	cfg           config.Config
	logger        *slog.Logger
	store         *storage.Store
	executor      executor
	registry      *Registry
	watchRegistry *WatchRegistry

	clientStates   map[uint64]*ClientState
	clientStatesMu sync.RWMutex

	listener     net.Listener
	listenerMu   sync.RWMutex
	shutdownOnce sync.Once
	handlerWG    sync.WaitGroup
}

// New constructs a server ready to listen for TCP clients.
func New(cfg config.Config, logger *slog.Logger, store *storage.Store, executor executor) *Server {
	srv := &Server{
		cfg:          cfg,
		logger:       logger,
		store:        store,
		executor:     executor,
		registry:     NewRegistry(),
		clientStates: make(map[uint64]*ClientState),
	}
	if provider, ok := executor.(watchRegistryProvider); ok {
		srv.watchRegistry = provider.WatchRegistry()
	}

	return srv
}

// ListenAndServe starts the TCP listener and blocks until shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.cfg.Address(), err)
	}

	s.setListener(listener)
	defer s.shutdown()

	s.logger.Info("RuneDB listening", "address", listener.Addr().String())
	s.store.StartEviction(ctx, s.cfg.EvictionInterval, s.cfg.EvictionSampleSize)

	stopShutdown := context.AfterFunc(ctx, s.shutdown)
	defer stopShutdown()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}

			var temporary interface{ Temporary() bool }
			if errors.As(err, &temporary) && temporary.Temporary() {
				s.logger.Warn("temporary accept error", "error", err)
				continue
			}

			return fmt.Errorf("server: accept connection: %w", err)
		}

		clientID := s.registry.Add(conn)
		s.createClientState(clientID)
		s.handlerWG.Add(1)
		go s.handleConnection(ctx, clientID, conn)
	}

	s.handlerWG.Wait()
	s.logger.Info("RuneDB stopped")
	return nil
}

// Addr returns the bound listener address once the server has started.
func (s *Server) Addr() string {
	s.listenerMu.RLock()
	defer s.listenerMu.RUnlock()

	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) setListener(listener net.Listener) {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	s.listener = listener
}

func (s *Server) shutdown() {
	s.shutdownOnce.Do(func() {
		s.listenerMu.RLock()
		listener := s.listener
		s.listenerMu.RUnlock()
		if listener != nil {
			_ = listener.Close()
		}
		s.registry.CloseAll()
		s.clearClientStates()
	})
}

func (s *Server) createClientState(clientID uint64) *ClientState {
	state := &ClientState{
		ID:            clientID,
		Authenticated: s.cfg.RequirePass == "",
	}
	state.SetWatchRegistry(s.watchRegistry)

	s.clientStatesMu.Lock()
	defer s.clientStatesMu.Unlock()

	s.clientStates[clientID] = state
	return state
}

func (s *Server) getClientState(clientID uint64) *ClientState {
	s.clientStatesMu.RLock()
	defer s.clientStatesMu.RUnlock()

	return s.clientStates[clientID]
}

func (s *Server) removeClientState(clientID uint64) {
	s.clientStatesMu.Lock()
	state := s.clientStates[clientID]
	delete(s.clientStates, clientID)
	s.clientStatesMu.Unlock()

	if state != nil {
		state.UnwatchAll()
	}
}

func (s *Server) clearClientStates() {
	s.clientStatesMu.Lock()
	states := make([]*ClientState, 0, len(s.clientStates))
	for _, state := range s.clientStates {
		states = append(states, state)
	}
	clear(s.clientStates)
	s.clientStatesMu.Unlock()

	for _, state := range states {
		state.UnwatchAll()
	}
}

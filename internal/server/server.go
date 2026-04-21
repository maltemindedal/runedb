package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/maltemindedal/runedb/internal/aof"
	"github.com/maltemindedal/runedb/internal/config"
	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/rdb"
	"github.com/maltemindedal/runedb/internal/storage"
)

type executor interface {
	ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error)
}

type watchRegistryProvider interface {
	WatchRegistry() *WatchRegistry
}

type pubSubRegistryProvider interface {
	PubSubRegistry() *PubSubRegistry
}

type replicationStateSetter interface {
	SetReplicationState(*ReplicationState)
}

type replicaRegistrySetter interface {
	SetReplicaRegistry(*ReplicaRegistry)
}

type authConfigSetter interface {
	SetRequirePass(string)
}

type aofRewriteTriggerSetter interface {
	SetAOFRewriteTrigger(func(context.Context) error)
}

type temporaryError interface {
	error
	Temporary() bool
}

// Server owns the TCP listener, active clients, and command execution pipeline.
type Server struct {
	cfg            config.Config
	logger         *slog.Logger
	store          *storage.Store
	executor       executor
	registry       *Registry
	replicaPeers   *ReplicaRegistry
	watchRegistry  *WatchRegistry
	pubSubRegistry *PubSubRegistry
	replication    *ReplicationState
	aofWriter      *aof.Writer
	runtimeCtx     context.Context

	clientStates   map[uint64]*ClientState
	clientStatesMu sync.RWMutex

	upstreamConn   net.Conn
	upstreamConnMu sync.Mutex

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
		replicaPeers: NewReplicaRegistry(),
		replication:  newReplicationState(),
		clientStates: make(map[uint64]*ClientState),
	}
	if store != nil {
		store.SetLogger(logger)
	}
	if provider, ok := executor.(watchRegistryProvider); ok {
		srv.watchRegistry = provider.WatchRegistry()
	}
	if provider, ok := executor.(pubSubRegistryProvider); ok {
		srv.pubSubRegistry = provider.PubSubRegistry()
	}
	if setter, ok := executor.(replicationStateSetter); ok {
		setter.SetReplicationState(srv.replication)
	}
	if setter, ok := executor.(replicaRegistrySetter); ok {
		setter.SetReplicaRegistry(srv.replicaPeers)
	}
	if setter, ok := executor.(authConfigSetter); ok {
		setter.SetRequirePass(cfg.RequirePass)
	}
	if setter, ok := executor.(aofRewriteTriggerSetter); ok {
		setter.SetAOFRewriteTrigger(srv.beginAOFRewrite)
	}

	return srv
}

// ListenAndServe starts the TCP listener and blocks until shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	s.runtimeCtx = ctx

	if s.cfg.IsReplica() {
		if _, err := s.cfg.ReplicaAddress(); err != nil {
			return fmt.Errorf("server: validate replica configuration: %w", err)
		}
	}
	if err := s.initializePersistence(ctx); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		if closeErr := s.closeAOFWriter(); closeErr != nil {
			s.logger.Warn("failed to close AOF writer after listen failure", "error", closeErr)
		}
		return fmt.Errorf("server: listen on %s: %w", s.cfg.Address(), err)
	}

	s.setListener(listener)
	defer s.shutdown()

	s.logger.Info("RuneDB listening", "address", listener.Addr().String())
	s.store.StartEviction(ctx, s.cfg.EvictionInterval, s.cfg.EvictionSampleSize)
	if s.cfg.IsReplica() {
		s.handlerWG.Add(1)
		go s.startReplicaLink(ctx, listener.Addr().String())
	}

	stopShutdown := context.AfterFunc(ctx, s.shutdown)
	defer stopShutdown()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}

			var temporary temporaryError
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
	if err := s.closeAOFWriter(); err != nil {
		return err
	}
	if err := s.persistSnapshot(); err != nil {
		return err
	}
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
		s.closeUpstreamConn()

		s.listenerMu.RLock()
		listener := s.listener
		s.listenerMu.RUnlock()
		if listener != nil {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.logger.Debug("failed to close listener during shutdown", "error", err)
			}
		}
		if err := s.registry.CloseAll(); err != nil {
			s.logger.Debug("failed to close one or more client connections during shutdown", "error", err)
		}
		s.clearClientStates()
	})
}

func (s *Server) persistSnapshot() error {
	if s == nil || s.store == nil || s.cfg.DumpPath == "" {
		return nil
	}

	entries, snapshotStats := s.store.SnapshotStrings()
	writeStats, err := rdb.SaveSnapshot(s.cfg.DumpPath, entries)
	if err != nil {
		s.logger.Error(
			"failed to save shutdown RDB snapshot",
			"path", s.cfg.DumpPath,
			"candidate_keys", len(entries),
			"error", err,
		)
		return fmt.Errorf("server: save shutdown snapshot to %q: %w", s.cfg.DumpPath, err)
	}

	if snapshotStats.SkippedUnsupportedKeys > 0 {
		s.logger.Warn(
			"shutdown snapshot skipped unsupported value types",
			"path", s.cfg.DumpPath,
			"skipped_unsupported_keys", snapshotStats.SkippedUnsupportedKeys,
			"exported_keys", snapshotStats.ExportedKeys,
		)
	}

	s.logger.Info(
		"saved shutdown RDB snapshot",
		"path", s.cfg.DumpPath,
		"written_keys", writeStats.WrittenKeys,
		"skipped_expired_keys", snapshotStats.SkippedExpiredKeys+writeStats.SkippedExpiredKeys,
	)

	return nil
}

func (s *Server) createClientState(clientID uint64) *ClientState {
	state := &ClientState{
		ID:            clientID,
		Authenticated: s.cfg.RequirePass == "",
	}
	state.SetWatchRegistry(s.watchRegistry)
	state.SetPubSubRegistry(s.pubSubRegistry)

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

func (s *Server) disconnectClientState(state *ClientState) {
	if state == nil {
		return
	}

	state.Disconnect()
}

func (s *Server) removeClientState(clientID uint64) {
	s.clientStatesMu.Lock()
	state := s.clientStates[clientID]
	delete(s.clientStates, clientID)
	s.clientStatesMu.Unlock()

	s.disconnectClientState(state)
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
		s.disconnectClientState(state)
	}
}

// ReplicaCount returns the number of replica peers connected to this server.
func (s *Server) ReplicaCount() int {
	return s.replicaPeers.Count()
}

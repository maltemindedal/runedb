package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"

	"github.com/maltemindedal/runedb/internal/protocol"
)

func (s *Server) handleConnection(ctx context.Context, clientID uint64, conn net.Conn) {
	defer s.handlerWG.Done()
	defer s.registry.Remove(clientID)

	parser := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)

	if state := s.getClientState(clientID); state != nil {
		state.BindResponseWriter(writer)
		ctx = WithClientState(ctx, state)
	}

	logger := s.logger.With("client_id", clientID, "remote_addr", conn.RemoteAddr().String())
	logger.Debug("client connected")
	defer logger.Debug("client disconnected")
	defer func() {
		if peer := s.replicaPeers.Remove(clientID); peer != nil {
			logger.Info("replica disconnected", "replica_id", clientID, "listening_port", peer.ListeningPort)
		}
	}()
	defer s.removeClientState(clientID)
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Debug("failed to close connection", "error", err)
		}
	}()

	stopClose := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Debug("failed to close connection after cancellation", "error", err)
		}
	})
	defer stopClose()

	for {
		value, err := parser.Parse()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}

			logger.Warn("failed to parse request", "error", err)
			if writeErr := s.writeClientResponses(ctx, writer, []protocol.Value{protocol.ErrorValue{Message: "ERR " + err.Error()}}); writeErr != nil {
				logger.Warn("failed to write parser error", "parse_error", err, "write_error", writeErr)
				return
			}
			continue
		}

		result, execErr := s.executor.ExecuteDetailed(ctx, value)
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return
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

		if err := s.writeClientResponses(ctx, writer, result.Responses); err != nil {
			s.finalizeMutationEffects(ctx, result.Durability, durabilityPayload, result.Propagation, logger)
			logger.Warn("failed to write response", "error", err, "propagation_frames", len(result.Propagation))
			return
		}
		if result.RegisterReplica {
			s.registerReplicaPeer(clientID, conn)
		}
		s.finalizeMutationEffects(ctx, result.Durability, durabilityPayload, result.Propagation, logger)
	}
}

func (s *Server) writeClientResponses(ctx context.Context, writer *bufio.Writer, values []protocol.Value) error {
	if state, ok := ClientStateFromContext(ctx); ok && state != nil {
		return state.WriteResponses(values)
	}

	return s.writeResponses(writer, values)
}

func (s *Server) writeResponses(writer *bufio.Writer, values []protocol.Value) error {
	for _, value := range values {
		if err := protocol.WriteValue(writer, value); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func responseError(err error) protocol.ErrorValue {
	prefix := "ERR"

	type respError interface {
		RESPErrorPrefix() string
	}

	var typed respError
	if errors.As(err, &typed) {
		prefix = typed.RESPErrorPrefix()
	}

	return protocol.ErrorValue{Message: prefix + " " + err.Error()}
}

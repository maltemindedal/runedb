package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"

	"github.com/maltemindedal/runedb/internal/protocol"
)

func (s *Server) handleConnection(ctx context.Context, clientID uint64, conn net.Conn) {
	defer s.handlerWG.Done()

	parser := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)

	if state := s.getClientState(clientID); state != nil {
		state.BindResponseWriter(writer)
		state.BindResponseConn(conn)
		if conn.RemoteAddr() != nil {
			state.SetRemoteAddr(conn.RemoteAddr().String())
		}
		ctx = WithClientState(ctx, state)
	}

	logger := s.logger.With("client_id", clientID, "remote_addr", conn.RemoteAddr().String())
	logger.Debug("client connected")
	defer s.teardownClient(clientID, logger)
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

		responses, registerReplica, execErr := s.executeClientRequest(ctx, clientID, conn, logger, value)
		if execErr != nil {
			return
		}

		if err := s.writeClientResponses(ctx, writer, responses); err != nil {
			logger.Warn("failed to write response", "error", err)
			return
		}
		// Register only after the handshake response reached the socket, so a
		// concurrently propagated command cannot precede the FULLRESYNC frames
		// in the replica's stream.
		if registerReplica {
			s.registerReplicaPeer(clientID, conn)
		}
	}
}

// executeClientRequest runs one parsed request through the command pipeline
// shared by both networking modes: monitor observation, execution, durability
// preparation and its persistence-failure downgrade, mutation-effect
// finalization, and the processed-command counter. It returns the RESP
// responses to deliver and whether the caller must register the connection as
// a replica peer once those responses have been written or buffered. A
// non-nil error is fatal for the connection.
func (s *Server) executeClientRequest(ctx context.Context, clientID uint64, conn ClientConn, logger *slog.Logger, request protocol.Value) ([]protocol.Value, bool, error) {
	if s.monitorRegistry.HasSubscribers() {
		s.broadcastMonitorEvent(observeCommand(request, clientID, conn))
	}

	result, execErr := s.executor.ExecuteDetailed(ctx, request)
	if execErr != nil {
		if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
			return nil, false, execErr
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

	s.finalizeMutationEffects(ctx, result.Durability, durabilityPayload, result.Propagation, logger)
	s.commandsProcessed.Add(1)

	return result.Responses, result.RegisterReplica, nil
}

func (s *Server) writeClientResponses(ctx context.Context, writer *bufio.Writer, values []protocol.Value) error {
	if state, ok := ClientStateFromContext(ctx); ok && state != nil {
		return state.WriteResponses(values)
	}

	return s.writeResponses(writer, values)
}

func (s *Server) writeResponses(writer *bufio.Writer, values []protocol.Value) error {
	if len(values) == 1 {
		if err := protocol.WriteValue(writer, values[0]); err != nil {
			return err
		}
		return writer.Flush()
	}

	payload, err := protocol.EncodeValues(values)
	if err != nil {
		return err
	}
	if _, err := writer.Write(payload); err != nil {
		return err
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

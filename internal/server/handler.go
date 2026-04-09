package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"

	"github.com/maltemindedal/godis/internal/protocol"
)

func (s *Server) handleConnection(ctx context.Context, clientID uint64, conn net.Conn) {
	defer s.handlerWG.Done()
	defer s.registry.Remove(clientID)
	defer s.removeClientState(clientID)

	if state := s.getClientState(clientID); state != nil {
		ctx = WithClientState(ctx, state)
	}

	logger := s.logger.With("client_id", clientID, "remote_addr", conn.RemoteAddr().String())
	logger.Debug("client connected")
	defer logger.Debug("client disconnected")
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

	parser := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)

	for {
		value, err := parser.Parse()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}

			logger.Warn("failed to parse request", "error", err)
			if writeErr := s.writeResponse(writer, protocol.ErrorValue{Message: "ERR " + err.Error()}); writeErr != nil {
				logger.Warn("failed to write parser error", "error", writeErr)
				return
			}
			continue
		}

		response, execErr := s.executor.Execute(ctx, value)
		if execErr != nil {
			logger.Debug("command execution failed", "error", execErr)
			response = responseError(execErr)
		}

		if err := s.writeResponse(writer, response); err != nil {
			logger.Warn("failed to write response", "error", err)
			return
		}
	}
}

func (s *Server) writeResponse(writer *bufio.Writer, value protocol.Value) error {
	if err := protocol.WriteValue(writer, value); err != nil {
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

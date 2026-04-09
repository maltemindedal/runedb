package command

import (
	"context"
	"log/slog"
	"strings"

	"github.com/maltemindedal/godis/internal/protocol"
	"github.com/maltemindedal/godis/internal/storage"
)

// Request is the parsed command sent by a client.
type Request struct {
	Name string
	Args [][]byte
}

// Handler executes a command against the current server state.
type Handler func(context.Context, *Request) (protocol.Value, error)

// Executor routes protocol frames to concrete command handlers.
type Executor struct {
	store    *storage.Store
	logger   *slog.Logger
	handlers map[string]Handler
}

// NewExecutor constructs a command executor with the Phase 1 command set.
func NewExecutor(store *storage.Store, logger *slog.Logger) *Executor {
	executor := &Executor{
		store:  store,
		logger: logger,
	}
	executor.handlers = map[string]Handler{
		"PING":   executor.handlePing,
		"ECHO":   executor.handleEcho,
		"SET":    executor.handleSet,
		"GET":    executor.handleGet,
		"DEL":    executor.handleDel,
		"INCR":   executor.handleIncr,
		"LPUSH":  executor.handleLPush,
		"RPUSH":  executor.handleRPush,
		"LRANGE": executor.handleLRange,
		"BLPOP":  executor.handleBLPop,
		"ZADD":   executor.handleZAdd,
		"ZRANGE": executor.handleZRange,
		"XADD":   executor.handleXAdd,
		"XREAD":  executor.handleXRead,
	}
	return executor
}

// Execute dispatches a parsed RESP frame to its command handler.
func (e *Executor) Execute(ctx context.Context, value protocol.Value) (protocol.Value, error) {
	request, err := DecodeRequest(value)
	if err != nil {
		return nil, err
	}

	handler, ok := e.handlers[request.Name]
	if !ok {
		return nil, ErrUnknownCommand(request.Name)
	}

	return handler(ctx, request)
}

// DecodeRequest converts a RESP array into a command request.
func DecodeRequest(value protocol.Value) (*Request, error) {
	array, ok := value.(protocol.Array)
	if !ok || array.Null {
		return nil, ErrProtocol("expected non-null RESP array")
	}
	if len(array.Elements) == 0 {
		return nil, ErrProtocol("expected array with at least one element")
	}

	parts := make([][]byte, 0, len(array.Elements))
	for _, element := range array.Elements {
		part, err := protocol.Bytes(element)
		if err != nil {
			return nil, ErrProtocol(err.Error())
		}
		parts = append(parts, part)
	}

	return &Request{
		Name: strings.ToUpper(string(parts[0])),
		Args: parts[1:],
	}, nil
}

package command

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/maltemindedal/godis/internal/protocol"
	"github.com/maltemindedal/godis/internal/server"
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
	store         *storage.Store
	logger        *slog.Logger
	watchRegistry *server.WatchRegistry
	handlers      map[string]Handler
}

// NewExecutor constructs a command executor with the Phase 1 command set.
func NewExecutor(store *storage.Store, logger *slog.Logger) *Executor {
	executor := &Executor{
		store:         store,
		logger:        logger,
		watchRegistry: server.NewWatchRegistry(),
	}
	executor.handlers = map[string]Handler{
		"WATCH":   executor.handleWatch,
		"MULTI":   executor.handleMulti,
		"EXEC":    executor.handleExec,
		"DISCARD": executor.handleDiscard,
		"PING":    executor.handlePing,
		"ECHO":    executor.handleEcho,
		"SET":     executor.handleSet,
		"GET":     executor.handleGet,
		"DEL":     executor.handleDel,
		"INCR":    executor.handleIncr,
		"LPUSH":   executor.handleLPush,
		"RPUSH":   executor.handleRPush,
		"LRANGE":  executor.handleLRange,
		"BLPOP":   executor.handleBLPop,
		"ZADD":    executor.handleZAdd,
		"ZRANGE":  executor.handleZRange,
		"XADD":    executor.handleXAdd,
		"XREAD":   executor.handleXRead,
	}
	return executor
}

// WatchRegistry exposes the shared optimistic-locking registry to the server.
func (e *Executor) WatchRegistry() *server.WatchRegistry {
	return e.watchRegistry
}

// Execute dispatches a parsed RESP frame to its command handler.
func (e *Executor) Execute(ctx context.Context, value protocol.Value) (protocol.Value, error) {
	request, err := DecodeRequest(value)
	if err != nil {
		return nil, err
	}

	return e.executeRequest(ctx, request, true)
}

func (e *Executor) executeRequest(ctx context.Context, request *Request, allowQueue bool) (protocol.Value, error) {
	if allowQueue {
		if queued, response, err := e.maybeQueueRequest(ctx, request); queued || err != nil {
			return response, err
		}
	}

	handler, ok := e.handlers[request.Name]
	if !ok {
		return nil, ErrUnknownCommand(request.Name)
	}

	return handler(ctx, request)
}

func (e *Executor) maybeQueueRequest(ctx context.Context, request *Request) (bool, protocol.Value, error) {
	state, ok := server.ClientStateFromContext(ctx)
	if !ok || !state.InTransactionActive() || isTransactionControlCommand(request.Name) {
		return false, nil, nil
	}

	state.EnqueueCommand(request.Name, request.Args)
	return true, protocol.SimpleString{Value: "QUEUED"}, nil
}

func isTransactionControlCommand(name string) bool {
	switch name {
	case "WATCH", "MULTI", "EXEC", "DISCARD":
		return true
	default:
		return false
	}
}

func responseErrorValue(err error) protocol.ErrorValue {
	prefix := "ERR"

	var typed RESPError
	if errors.As(err, &typed) {
		prefix = typed.RESPErrorPrefix()
	}

	return protocol.ErrorValue{Message: prefix + " " + err.Error()}
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

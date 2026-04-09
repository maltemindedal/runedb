package command

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/server"
	"github.com/maltemindedal/runedb/internal/storage"
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

// NewExecutor constructs a command executor with the currently supported command set.
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

func (e *Executor) validateQueueableRequest(request *Request) error {
	if _, ok := e.handlers[request.Name]; !ok {
		return ErrUnknownCommand(request.Name)
	}

	switch request.Name {
	case "WATCH":
		if len(request.Args) == 0 {
			return wrongNumberOfArgumentsError("WATCH")
		}
	case "MULTI":
		if len(request.Args) != 0 {
			return wrongNumberOfArgumentsError("MULTI")
		}
	case "EXEC":
		if len(request.Args) != 0 {
			return wrongNumberOfArgumentsError("EXEC")
		}
	case "DISCARD":
		if len(request.Args) != 0 {
			return wrongNumberOfArgumentsError("DISCARD")
		}
	case "PING":
		if len(request.Args) > 1 {
			return wrongNumberOfArgumentsError("PING")
		}
	case "ECHO":
		if len(request.Args) != 1 {
			return wrongNumberOfArgumentsError("ECHO")
		}
	case "SET":
		if len(request.Args) < 2 {
			return wrongNumberOfArgumentsError("SET")
		}
		if _, err := storage.ParseExpiryMillis(request.Args[2:]); err != nil {
			switch err {
			case storage.ErrSyntax:
				return ErrSyntaxError()
			case storage.ErrInvalidExpireTime:
				return ErrInvalidExpireTimeError()
			default:
				return err
			}
		}
	case "GET":
		if len(request.Args) != 1 {
			return wrongNumberOfArgumentsError("GET")
		}
	case "DEL":
		if len(request.Args) < 1 {
			return wrongNumberOfArgumentsError("DEL")
		}
	case "INCR":
		if len(request.Args) != 1 {
			return wrongNumberOfArgumentsError("INCR")
		}
	case "LPUSH":
		if len(request.Args) < 2 {
			return wrongNumberOfArgumentsError("LPUSH")
		}
	case "RPUSH":
		if len(request.Args) < 2 {
			return wrongNumberOfArgumentsError("RPUSH")
		}
	case "LRANGE":
		if len(request.Args) != 3 {
			return wrongNumberOfArgumentsError("LRANGE")
		}
		if _, err := parseIntegerArgument(request.Args[1]); err != nil {
			return err
		}
		if _, err := parseIntegerArgument(request.Args[2]); err != nil {
			return err
		}
	case "BLPOP":
		if len(request.Args) != 1 {
			return wrongNumberOfArgumentsError("BLPOP")
		}
	case "ZADD":
		if len(request.Args) < 3 || len(request.Args)%2 == 0 {
			return wrongNumberOfArgumentsError("ZADD")
		}
		for i := 1; i < len(request.Args); i += 2 {
			if _, err := parseFloatArgument(request.Args[i]); err != nil {
				return err
			}
		}
	case "ZRANGE":
		if len(request.Args) != 3 && len(request.Args) != 4 {
			return wrongNumberOfArgumentsError("ZRANGE")
		}
		if len(request.Args) == 4 && !strings.EqualFold(string(request.Args[3]), "WITHSCORES") {
			return ErrSyntaxError()
		}
		if _, err := parseIntegerArgument(request.Args[1]); err != nil {
			return err
		}
		if _, err := parseIntegerArgument(request.Args[2]); err != nil {
			return err
		}
	case "XADD":
		if len(request.Args) < 4 || len(request.Args)%2 != 0 {
			return wrongNumberOfArgumentsError("XADD")
		}
		if err := storage.ValidateXAddID(string(request.Args[1])); err != nil {
			if errors.Is(err, storage.ErrInvalidStreamID) {
				return ErrInvalidStreamIDError()
			}
			return err
		}
	case "XREAD":
		if len(request.Args) == 0 {
			return wrongNumberOfArgumentsError("XREAD")
		}
		if !strings.EqualFold(string(request.Args[0]), "STREAMS") {
			return ErrSyntaxError()
		}
		if len(request.Args) != 3 {
			return ErrSyntaxError()
		}
		if err := storage.ValidateXReadID(string(request.Args[2])); err != nil {
			if errors.Is(err, storage.ErrInvalidStreamID) {
				return ErrInvalidStreamIDError()
			}
			return err
		}
	}

	return nil
}

func (e *Executor) maybeQueueRequest(ctx context.Context, request *Request) (bool, protocol.Value, error) {
	state, ok := server.ClientStateFromContext(ctx)
	if !ok || !state.InTransactionActive() || isTransactionControlCommand(request.Name) {
		return false, nil, nil
	}
	if err := e.validateQueueableRequest(request); err != nil {
		state.MarkTransactionDirty()
		return false, nil, err
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

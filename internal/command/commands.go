package command

import (
	"context"

	"github.com/maltemindedal/godis/internal/protocol"
	"github.com/maltemindedal/godis/internal/storage"
)

func (e *Executor) handlePing(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) == 1 {
		return protocol.BulkString{Data: clone(request.Args[0])}, nil
	}
	if len(request.Args) > 1 {
		return nil, wrongNumberOfArgumentsError("PING")
	}

	return protocol.SimpleString{Value: "PONG"}, nil
}

func (e *Executor) handleEcho(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("ECHO")
	}

	return protocol.BulkString{Data: clone(request.Args[0])}, nil
}

func (e *Executor) handleSet(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("SET")
	}

	expiresAt, err := storage.ParseExpiryMillis(request.Args[2:])
	if err != nil {
		switch err {
		case storage.ErrSyntax:
			return nil, ErrSyntaxError()
		case storage.ErrInvalidExpireTime:
			return nil, ErrInvalidExpireTimeError()
		default:
			return nil, err
		}
	}

	e.store.Set(string(request.Args[0]), request.Args[1], expiresAt)
	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleGet(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("GET")
	}

	value, ok := e.store.Get(string(request.Args[0]))
	if !ok {
		return protocol.BulkString{Null: true}, nil
	}

	return protocol.BulkString{Data: value}, nil
}

func (e *Executor) handleDel(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 1 {
		return nil, wrongNumberOfArgumentsError("DEL")
	}

	removed := int64(0)
	for _, arg := range request.Args {
		if e.store.Delete(string(arg)) {
			removed++
		}
	}

	return protocol.Integer{Value: removed}, nil
}

func (e *Executor) handleIncr(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("INCR")
	}

	value, err := e.store.Increment(string(request.Args[0]))
	if err != nil {
		switch err {
		case storage.ErrWrongType:
			return nil, ErrWrongTypeError()
		case storage.ErrValueNotInteger:
			return nil, ErrValueNotIntegerError()
		default:
			return nil, err
		}
	}

	return protocol.Integer{Value: value}, nil
}

func clone(value []byte) []byte {
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied
}

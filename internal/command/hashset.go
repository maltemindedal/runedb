package command

import (
	"context"
	"errors"
	"strconv"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func (e *Executor) handleLPop(_ context.Context, request *Request) (protocol.Value, error) {
	return e.popList(request, "LPOP", true)
}

func (e *Executor) handleRPop(_ context.Context, request *Request) (protocol.Value, error) {
	return e.popList(request, "RPOP", false)
}

func (e *Executor) popList(request *Request, name string, left bool) (protocol.Value, error) {
	if len(request.Args) != 1 && len(request.Args) != 2 {
		return nil, wrongNumberOfArgumentsError(name)
	}

	key := string(request.Args[0])

	if len(request.Args) == 1 {
		var (
			value []byte
			ok    bool
			err   error
		)
		if left {
			value, ok, err = e.store.LeftPop(key)
		} else {
			value, ok, err = e.store.RightPop(key)
		}
		if err != nil {
			if errors.Is(err, storage.ErrWrongType) {
				return nil, ErrWrongTypeError()
			}
			return nil, err
		}
		if !ok {
			return protocol.BulkString{Null: true}, nil
		}

		e.touchWatchKeys(key)
		return protocol.BulkString{Data: value}, nil
	}

	count, err := parseIntegerArgument(request.Args[1])
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, newRESPMessageError("ERR", "value is out of range, must be positive")
	}

	var (
		values [][]byte
		ok     bool
	)
	if left {
		values, ok, err = e.store.LeftPopN(key, count)
	} else {
		values, ok, err = e.store.RightPopN(key, count)
	}
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		if errors.Is(err, storage.ErrSyntax) {
			return nil, ErrSyntaxError()
		}
		return nil, err
	}
	if !ok {
		return protocol.Array{Null: true}, nil
	}

	if count > 0 && len(values) > 0 {
		e.touchWatchKeys(key)
	}
	return listResponse(values), nil
}

func (e *Executor) handleHSet(ctx context.Context, request *Request) (protocol.Value, error) {
	if err := validateHSetRequest(request); err != nil {
		return nil, err
	}

	key := string(request.Args[0])
	pairs := make([]storage.HashFieldValue, 0, (len(request.Args)-1)/2)
	for i := 1; i < len(request.Args); i += 2 {
		pairs = append(pairs, storage.HashFieldValue{
			Field: string(request.Args[i]),
			Value: request.Args[i+1],
		})
	}

	added, evicted, err := e.store.HSetWithEviction(key, pairs)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleHGet(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 2 {
		return nil, wrongNumberOfArgumentsError("HGET")
	}

	value, ok, err := e.store.HGet(string(request.Args[0]), string(request.Args[1]))
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}
	if !ok {
		return protocol.BulkString{Null: true}, nil
	}

	return protocol.BulkString{Data: value}, nil
}

func (e *Executor) handleHDel(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("HDEL")
	}

	key := string(request.Args[0])
	fields := make([]string, 0, len(request.Args)-1)
	for _, arg := range request.Args[1:] {
		fields = append(fields, string(arg))
	}

	removed, err := e.store.HDel(key, fields)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrWrongType):
			return nil, ErrWrongTypeError()
		case errors.Is(err, storage.ErrSyntax):
			return nil, ErrSyntaxError()
		default:
			return nil, err
		}
	}

	if removed > 0 {
		e.touchWatchKeys(key)
	}
	return protocol.Integer{Value: removed}, nil
}

func (e *Executor) handleHGetAll(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("HGETALL")
	}

	entries, err := e.store.HGetAll(string(request.Args[0]))
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	elements := make([]protocol.Value, 0, len(entries)*2)
	for _, entry := range entries {
		elements = append(elements, protocol.BulkString{Data: []byte(entry.Field)})
		elements = append(elements, protocol.BulkString{Data: entry.Value})
	}

	return protocol.Array{Elements: elements}, nil
}

func (e *Executor) handleSAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("SADD")
	}

	key := string(request.Args[0])
	added, evicted, err := e.store.SAddWithEviction(key, request.Args[1:])
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleSIsMember(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 2 {
		return nil, wrongNumberOfArgumentsError("SISMEMBER")
	}

	exists, err := e.store.SIsMember(string(request.Args[0]), request.Args[1])
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	if exists {
		return protocol.Integer{Value: 1}, nil
	}
	return protocol.Integer{Value: 0}, nil
}

func (e *Executor) handleSRem(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("SREM")
	}

	key := string(request.Args[0])
	removed, err := e.store.SRem(key, request.Args[1:])
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrWrongType):
			return nil, ErrWrongTypeError()
		case errors.Is(err, storage.ErrSyntax):
			return nil, ErrSyntaxError()
		default:
			return nil, err
		}
	}

	if removed > 0 {
		e.touchWatchKeys(key)
	}
	return protocol.Integer{Value: removed}, nil
}

func (e *Executor) handleSMembers(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("SMEMBERS")
	}

	members, err := e.store.SMembers(string(request.Args[0]))
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	elements := make([]protocol.Value, 0, len(members))
	for _, member := range members {
		elements = append(elements, protocol.BulkString{Data: member})
	}
	return protocol.Array{Elements: elements}, nil
}

func validateHSetRequest(request *Request) error {
	if len(request.Args) < 3 || len(request.Args)%2 == 0 {
		return wrongNumberOfArgumentsError("HSET")
	}
	return nil
}

func validateLPopRequest(request *Request) error {
	return validatePopRequest(request, "LPOP")
}

func validateRPopRequest(request *Request) error {
	return validatePopRequest(request, "RPOP")
}

func validatePopRequest(request *Request, name string) error {
	if len(request.Args) != 1 && len(request.Args) != 2 {
		return wrongNumberOfArgumentsError(name)
	}
	if len(request.Args) == 2 {
		count, err := strconv.ParseInt(string(request.Args[1]), 10, 64)
		if err != nil {
			return ErrValueNotIntegerError()
		}
		if count < 0 {
			return newRESPMessageError("ERR", "value is out of range, must be positive")
		}
	}
	return nil
}

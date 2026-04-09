package command

import (
	"context"
	"math"
	"strconv"
	"strings"

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

	value, ok, err := e.store.Get(string(request.Args[0]))
	if err != nil {
		if err == storage.ErrWrongType {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}
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

func (e *Executor) handleLPush(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("LPUSH")
	}

	length, err := e.store.LeftPush(string(request.Args[0]), request.Args[1:])
	if err != nil {
		if err == storage.ErrWrongType {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	return protocol.Integer{Value: length}, nil
}

func (e *Executor) handleRPush(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("RPUSH")
	}

	length, err := e.store.RightPush(string(request.Args[0]), request.Args[1:])
	if err != nil {
		if err == storage.ErrWrongType {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	return protocol.Integer{Value: length}, nil
}

func (e *Executor) handleLRange(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 3 {
		return nil, wrongNumberOfArgumentsError("LRANGE")
	}

	start, err := parseIntegerArgument(request.Args[1])
	if err != nil {
		return nil, err
	}
	stop, err := parseIntegerArgument(request.Args[2])
	if err != nil {
		return nil, err
	}

	values, err := e.store.ListRange(string(request.Args[0]), start, stop)
	if err != nil {
		if err == storage.ErrWrongType {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	return listResponse(values), nil
}

func (e *Executor) handleBLPop(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("BLPOP")
	}

	key := string(request.Args[0])
	for {
		waiter := e.store.SubscribeListPush(key)

		value, ok, err := e.store.LeftPop(key)
		if err != nil {
			e.store.UnsubscribeListPush(key, waiter)
			if err == storage.ErrWrongType {
				return nil, ErrWrongTypeError()
			}
			return nil, err
		}
		if ok {
			e.store.UnsubscribeListPush(key, waiter)
			return protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: clone(request.Args[0])},
				protocol.BulkString{Data: value},
			}}, nil
		}

		select {
		case <-waiter:
			e.store.UnsubscribeListPush(key, waiter)
		case <-ctx.Done():
			e.store.UnsubscribeListPush(key, waiter)
			return nil, ctx.Err()
		}
	}
}

func (e *Executor) handleZAdd(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 3 || len(request.Args)%2 == 0 {
		return nil, wrongNumberOfArgumentsError("ZADD")
	}

	entries := make([]storage.ZSetEntry, 0, (len(request.Args)-1)/2)
	for i := 1; i < len(request.Args); i += 2 {
		score, err := parseFloatArgument(request.Args[i])
		if err != nil {
			return nil, err
		}

		entries = append(entries, storage.ZSetEntry{
			Member: clone(request.Args[i+1]),
			Score:  score,
		})
	}

	added, err := e.store.ZAdd(string(request.Args[0]), entries)
	if err != nil {
		switch err {
		case storage.ErrWrongType:
			return nil, ErrWrongTypeError()
		case storage.ErrSyntax:
			return nil, ErrSyntaxError()
		default:
			return nil, err
		}
	}

	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleZRange(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 3 && len(request.Args) != 4 {
		return nil, wrongNumberOfArgumentsError("ZRANGE")
	}

	withScores := false
	if len(request.Args) == 4 {
		if !strings.EqualFold(string(request.Args[3]), "WITHSCORES") {
			return nil, ErrSyntaxError()
		}
		withScores = true
	}

	start, err := parseIntegerArgument(request.Args[1])
	if err != nil {
		return nil, err
	}
	stop, err := parseIntegerArgument(request.Args[2])
	if err != nil {
		return nil, err
	}

	entries, err := e.store.ZRange(string(request.Args[0]), start, stop)
	if err != nil {
		if err == storage.ErrWrongType {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	return zsetResponse(entries, withScores), nil
}

func (e *Executor) handleXAdd(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 4 || len(request.Args)%2 != 0 {
		return nil, wrongNumberOfArgumentsError("XADD")
	}

	id, err := e.store.XAdd(string(request.Args[0]), string(request.Args[1]), request.Args[2:])
	if err != nil {
		switch err {
		case storage.ErrWrongType:
			return nil, ErrWrongTypeError()
		case storage.ErrSyntax:
			return nil, ErrSyntaxError()
		case storage.ErrInvalidStreamID:
			return nil, ErrInvalidStreamIDError()
		case storage.ErrStreamIDTooSmall:
			return nil, ErrStreamIDTooSmallError()
		default:
			return nil, err
		}
	}

	return protocol.BulkString{Data: []byte(id)}, nil
}

func (e *Executor) handleXRead(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) == 0 {
		return nil, wrongNumberOfArgumentsError("XREAD")
	}
	if !strings.EqualFold(string(request.Args[0]), "STREAMS") {
		return nil, ErrSyntaxError()
	}
	if len(request.Args) != 3 {
		return nil, ErrSyntaxError()
	}

	entries, err := e.store.XRead(string(request.Args[1]), string(request.Args[2]))
	if err != nil {
		switch err {
		case storage.ErrWrongType:
			return nil, ErrWrongTypeError()
		case storage.ErrInvalidStreamID:
			return nil, ErrInvalidStreamIDError()
		default:
			return nil, err
		}
	}

	return streamReadResponse(request.Args[1], entries), nil
}

func listResponse(values [][]byte) protocol.Array {
	elements := make([]protocol.Value, 0, len(values))
	for _, value := range values {
		elements = append(elements, protocol.BulkString{Data: clone(value)})
	}

	return protocol.Array{Elements: elements}
}

func zsetResponse(entries []storage.ZSetEntry, withScores bool) protocol.Array {
	elements := make([]protocol.Value, 0, len(entries))
	if withScores {
		elements = make([]protocol.Value, 0, len(entries)*2)
	}

	for _, entry := range entries {
		elements = append(elements, protocol.BulkString{Data: clone(entry.Member)})
		if withScores {
			elements = append(elements, protocol.BulkString{Data: formatFloatScore(entry.Score)})
		}
	}

	return protocol.Array{Elements: elements}
}

func streamReadResponse(key []byte, entries []storage.StreamEntry) protocol.Array {
	if len(entries) == 0 {
		return protocol.Array{Elements: []protocol.Value{}}
	}

	streamEntries := make([]protocol.Value, 0, len(entries))
	for _, entry := range entries {
		streamEntries = append(streamEntries, protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte(entry.ID)},
			listResponse(entry.Values),
		}})
	}

	return protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: clone(key)},
			protocol.Array{Elements: streamEntries},
		}},
	}}
}

func parseIntegerArgument(raw []byte) (int64, error) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, ErrValueNotIntegerError()
	}

	return value, nil
}

func parseFloatArgument(raw []byte) (float64, error) {
	value, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, ErrValueNotFloatError()
	}

	return value, nil
}

func formatFloatScore(score float64) []byte {
	return []byte(strconv.FormatFloat(score, 'g', -1, 64))
}

func clone(value []byte) []byte {
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied
}

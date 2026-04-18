package command

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/rdb"
	"github.com/maltemindedal/runedb/internal/server"
	"github.com/maltemindedal/runedb/internal/storage"
)

func (e *Executor) handleWatch(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) == 0 {
		return nil, wrongNumberOfArgumentsError("WATCH")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if state.InTransactionActive() {
		return nil, ErrWatchInsideMultiError()
	}

	keys := make([]string, 0, len(request.Args))
	for _, arg := range request.Args {
		keys = append(keys, string(arg))
	}
	state.WatchKeys(keys...)

	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleSubscribe(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if err := validateSubscribeRequest(request); err != nil {
		return server.ExecuteResult{}, err
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if state.InTransactionActive() {
		return server.ExecuteResult{}, ErrSubscribeInsideMultiError()
	}

	responses := make([]protocol.Value, 0, len(request.Args))
	for _, arg := range request.Args {
		channel := string(arg)
		state.SubscribeChannel(channel)
		responses = append(responses, pubSubAckResponse("subscribe", protocol.BulkString{Data: clone(arg)}, state.SubscriptionCount()))
	}

	return server.MultiResponse(responses...), nil
}

func (e *Executor) handleUnsubscribe(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if err := validateUnsubscribeRequest(request); err != nil {
		return server.ExecuteResult{}, err
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if state.InTransactionActive() {
		return server.ExecuteResult{}, ErrSubscribeInsideMultiError()
	}

	channels := make([]string, 0, len(request.Args))
	if len(request.Args) == 0 {
		channels = state.SubscribedChannels()
		if len(channels) == 0 {
			return server.MultiResponse(pubSubAckResponse("unsubscribe", protocol.BulkString{Null: true}, 0)), nil
		}
	} else {
		for _, arg := range request.Args {
			channels = append(channels, string(arg))
		}
	}

	responses := make([]protocol.Value, 0, len(channels))
	for _, channel := range channels {
		state.UnsubscribeChannel(channel)
		responses = append(responses, pubSubAckResponse("unsubscribe", protocol.BulkString{Data: []byte(channel)}, state.SubscriptionCount()))
	}

	return server.MultiResponse(responses...), nil
}

func (e *Executor) handleMulti(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 0 {
		return nil, wrongNumberOfArgumentsError("MULTI")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if state.IsSubscribed() {
		return nil, ErrSubscribedModeOnlyError()
	}
	if !state.BeginTransaction() {
		return nil, ErrMultiNestedError()
	}

	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleExec(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if len(request.Args) != 0 {
		return server.ExecuteResult{}, wrongNumberOfArgumentsError("EXEC")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if !state.InTransactionActive() {
		return server.ExecuteResult{}, ErrExecWithoutMultiError()
	}
	if state.TransactionDirty() {
		state.ResetTransaction()
		state.UnwatchAll()
		return server.ExecuteResult{}, ErrExecAbortError()
	}
	if state.TransactionFailed() {
		state.ResetTransaction()
		state.UnwatchAll()
		return server.SingleResponse(protocol.Array{Null: true}), nil
	}
	state.UnwatchAll()

	queued := state.DrainTransaction()
	responses := make([]protocol.Value, 0, len(queued))
	propagation := make([]protocol.Value, 0, len(queued))
	for _, queuedCommand := range queued {
		result, execErr := e.executeRequestDetailed(ctx, &Request{Name: queuedCommand.Name, Args: queuedCommand.Args}, false)
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return server.ExecuteResult{}, execErr
			}
			responses = append(responses, responseErrorValue(execErr))
			continue
		}
		if len(result.Responses) != 1 {
			responses = append(responses, protocol.ErrorValue{Message: "ERR queued command returned an invalid response"})
			continue
		}
		responses = append(responses, result.Responses[0])
		propagation = append(propagation, result.Propagation...)
	}

	result := server.SingleResponse(protocol.Array{Elements: responses})
	result.Propagation = propagation
	return result, nil
}

func (e *Executor) handleReplConf(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if len(request.Args) != 2 {
		return server.ExecuteResult{}, wrongNumberOfArgumentsError("REPLCONF")
	}
	subcommand := strings.ToUpper(string(request.Args[0]))

	switch subcommand {
	case "LISTENING-PORT":
		port, err := parseReplicationPortArgument(request.Args[1])
		if err != nil {
			return server.ExecuteResult{}, ErrSyntaxError()
		}

		if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
			state.SetReplicaListeningPort(port)
		}

		return server.SingleResponse(protocol.SimpleString{Value: "OK"}), nil
	case "GETACK":
		if !server.IsReplicationOrigin(ctx) || string(request.Args[1]) != "*" {
			return server.ExecuteResult{}, ErrSyntaxError()
		}

		ackOffset := int64(0)
		if e.replication != nil {
			ackOffset = e.replication.ReplicaOffset()
		}

		result := server.ExecuteResult{}
		result.UpstreamReplies = []protocol.Value{propagationFrame(&Request{
			Name: "REPLCONF",
			Args: [][]byte{[]byte("ACK"), []byte(strconv.FormatInt(ackOffset, 10))},
		})}
		return result, nil
	case "ACK":
		ackOffset, err := parseIntegerArgument(request.Args[1])
		if err != nil || ackOffset < 0 {
			return server.ExecuteResult{}, ErrSyntaxError()
		}

		if state, ok := server.ClientStateFromContext(ctx); ok && state != nil && state.IsReplica() && e.replicaPeers != nil {
			if updated := e.replicaPeers.UpdateAck(state.ID, ackOffset); updated {
				e.logger.Debug("replica acknowledged offset", "replica_id", state.ID, "ack_offset", ackOffset)
			}
		}

		return server.ExecuteResult{}, nil
	default:
		return server.ExecuteResult{}, ErrSyntaxError()
	}
}

func (e *Executor) handlePSync(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if len(request.Args) != 2 {
		return server.ExecuteResult{}, wrongNumberOfArgumentsError("PSYNC")
	}
	if string(request.Args[0]) != "?" || string(request.Args[1]) != "-1" {
		return server.ExecuteResult{}, ErrSyntaxError()
	}
	if e.replication == nil || e.replication.MasterReplicationID == "" {
		return server.ExecuteResult{}, fmt.Errorf("replication state unavailable")
	}

	if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
		state.PromoteToReplica()
		e.logger.Info("serving full resync to replica", "replica_id", state.ID, "master_replid", e.replication.MasterReplicationID)
	}

	result := server.MultiResponse(
		protocol.SimpleString{Value: fmt.Sprintf("FULLRESYNC %s 0", e.replication.MasterReplicationID)},
		protocol.BulkString{Data: rdb.EmptySnapshot()},
	)
	result.RegisterReplica = true
	return result, nil
}

func (e *Executor) handleWait(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	replicas, timeoutMillis, err := parseWaitArguments(request)
	if err != nil {
		return server.ExecuteResult{}, err
	}

	targetOffset := e.waitTargetOffset(ctx)
	startedAt := time.Now()

	ackedReplicas := e.countReplicasAtOrAbove(targetOffset)
	e.logger.Debug(
		"WAIT requested",
		"target_replicas", replicas,
		"timeout_millis", timeoutMillis,
		"target_offset", targetOffset,
		"currently_acked", ackedReplicas,
	)
	if ackedReplicas >= int(replicas) || timeoutMillis == 0 {
		return e.finishWaitResult(replicas, ackedReplicas, targetOffset, startedAt, false), nil
	}

	if err := e.requestReplicaAcknowledgements(); err != nil {
		return server.ExecuteResult{}, err
	}

	return e.waitForReplicaAcknowledgements(ctx, replicas, timeoutMillis, targetOffset, startedAt)
}

func parseWaitArguments(request *Request) (int64, int64, error) {
	if len(request.Args) != 2 {
		return 0, 0, wrongNumberOfArgumentsError("WAIT")
	}

	replicas, err := parseIntegerArgument(request.Args[0])
	if err != nil || replicas < 0 {
		return 0, 0, ErrValueNotIntegerError()
	}
	timeoutMillis, err := parseIntegerArgument(request.Args[1])
	if err != nil || timeoutMillis < 0 {
		return 0, 0, ErrValueNotIntegerError()
	}

	return replicas, timeoutMillis, nil
}

func (e *Executor) waitTargetOffset(ctx context.Context) int64 {
	if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
		return state.LastWriteReplicationOffset()
	}
	if e.replication != nil {
		return e.replication.MasterOffset()
	}

	return 0
}

func (e *Executor) waitForReplicaAcknowledgements(ctx context.Context, replicas int64, timeoutMillis int64, targetOffset int64, startedAt time.Time) (server.ExecuteResult, error) {
	timer := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer timer.Stop()

	for {
		ackedReplicas, notified := e.countReplicasAtOrAboveWithNotify(targetOffset)
		if ackedReplicas >= int(replicas) {
			return e.finishWaitResult(replicas, ackedReplicas, targetOffset, startedAt, false), nil
		}

		select {
		case <-ctx.Done():
			return server.ExecuteResult{}, ctx.Err()
		case <-timer.C:
			finalAcked := e.countReplicasAtOrAbove(targetOffset)
			return e.finishWaitResult(replicas, finalAcked, targetOffset, startedAt, true), nil
		case <-notified:
		}
	}
}

func (e *Executor) finishWaitResult(replicas int64, ackedReplicas int, targetOffset int64, startedAt time.Time, timedOut bool) server.ExecuteResult {
	e.logger.Info(
		"WAIT completed",
		"target_replicas", replicas,
		"acked_replicas", ackedReplicas,
		"target_offset", targetOffset,
		"timed_out", timedOut,
		"elapsed", time.Since(startedAt),
	)

	return server.SingleResponse(protocol.Integer{Value: int64(ackedReplicas)})
}

func (e *Executor) handleDiscard(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 0 {
		return nil, wrongNumberOfArgumentsError("DISCARD")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if !state.InTransactionActive() {
		return nil, ErrDiscardWithoutMultiError()
	}

	state.ResetTransaction()
	state.UnwatchAll()
	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleAuth(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("AUTH")
	}
	if e.requirePass == "" {
		return nil, ErrAuthNotConfiguredError()
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if !secretsEqual(e.requirePass, request.Args[0]) {
		return nil, ErrWrongPassError()
	}

	state.SetAuthenticated(true)
	return protocol.SimpleString{Value: "OK"}, nil
}

func secretsEqual(expected string, actual []byte) bool {
	expectedDigest := sha256.Sum256([]byte(expected))
	actualDigest := sha256.Sum256(actual)

	return subtle.ConstantTimeCompare(expectedDigest[:], actualDigest[:]) == 1
}

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

func (e *Executor) handlePublish(_ context.Context, request *Request) (protocol.Value, error) {
	if err := validatePublishRequest(request); err != nil {
		return nil, err
	}
	if e.pubSubRegistry == nil {
		return protocol.Integer{Value: 0}, nil
	}

	channel := string(request.Args[0])
	message := pubSubMessageResponse(request.Args[0], request.Args[1])
	subscribers := e.pubSubRegistry.Subscribers(channel)
	if len(subscribers) == 0 {
		return protocol.Integer{Value: 0}, nil
	}

	encodedMessage, err := protocol.Encode(message)
	if err != nil {
		return nil, fmt.Errorf("encode pub/sub message: %w", err)
	}

	matched := int64(0)
	for _, subscriber := range subscribers {
		if subscriber == nil {
			continue
		}
		if err := subscriber.WriteEncoded(encodedMessage); err != nil {
			e.logger.Warn(
				"failed to deliver pub/sub message",
				"channel", channel,
				"subscriber_id", subscriber.ID,
				"error", err,
			)
			subscriber.UnsubscribeAll()
			continue
		}
		matched++
	}

	return protocol.Integer{Value: matched}, nil
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
	e.touchWatchKeys(string(request.Args[0]))
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
	touched := make([]string, 0, len(request.Args))
	if len(request.Args) == 1 {
		key := string(request.Args[0])
		if e.store.Delete(key) {
			removed = 1
			touched = append(touched, key)
		}
	} else {
		keys := make([]string, 0, len(request.Args))
		for _, arg := range request.Args {
			key := string(arg)
			keys = append(keys, key)
		}

		touched = e.store.DeleteMany(keys)
		removed = int64(len(touched))
	}
	if len(touched) > 0 {
		e.touchWatchKeys(touched...)
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

	e.touchWatchKeys(string(request.Args[0]))
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

	e.touchWatchKeys(string(request.Args[0]))
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

	e.touchWatchKeys(string(request.Args[0]))
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
			e.touchWatchKeys(key)
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

	e.touchWatchKeys(string(request.Args[0]))
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

	e.touchWatchKeys(string(request.Args[0]))
	return protocol.TextBulkString{Value: id}, nil
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
		elements = append(elements, protocol.BulkString{Data: value})
	}

	return protocol.Array{Elements: elements}
}

func zsetResponse(entries []storage.ZSetRangeEntry, withScores bool) protocol.Array {
	elements := make([]protocol.Value, 0, len(entries))
	if withScores {
		elements = make([]protocol.Value, 0, len(entries)*2)
	}

	for _, entry := range entries {
		elements = append(elements, protocol.TextBulkString{Value: entry.Member})
		if withScores {
			elements = append(elements, protocol.TextBulkString{Value: formatFloatScore(entry.Score)})
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
			protocol.TextBulkString{Value: entry.ID},
			listResponse(entry.Values),
		}})
	}

	return protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: key},
			protocol.Array{Elements: streamEntries},
		}},
	}}
}

func pubSubAckResponse(kind string, channel protocol.Value, count int) protocol.Array {
	return protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: kind},
		channel,
		protocol.Integer{Value: int64(count)},
	}}
}

func pubSubMessageResponse(channel []byte, payload []byte) protocol.Array {
	return protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: clone(channel)},
		protocol.BulkString{Data: clone(payload)},
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

func formatFloatScore(score float64) string {
	return strconv.FormatFloat(score, 'g', -1, 64)
}

func parseReplicationPortArgument(raw []byte) (int, error) {
	port, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port %d", port)
	}

	return port, nil
}

func clone(value []byte) []byte {
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied
}

func clientStateFromContext(ctx context.Context) (*server.ClientState, error) {
	state, ok := server.ClientStateFromContext(ctx)
	if !ok || state == nil {
		return nil, fmt.Errorf("client state unavailable")
	}

	return state, nil
}

func (e *Executor) touchWatchKeys(keys ...string) {
	if e.watchRegistry == nil || len(keys) == 0 {
		return
	}

	e.watchRegistry.Touch(keys...)
}

func (e *Executor) countReplicasAtOrAbove(targetOffset int64) int {
	if e.replicaPeers == nil {
		return 0
	}

	return e.replicaPeers.CountReplicasAtOrAbove(targetOffset)
}

func (e *Executor) countReplicasAtOrAboveWithNotify(targetOffset int64) (int, <-chan struct{}) {
	if e.replicaPeers == nil {
		return 0, nil
	}

	return e.replicaPeers.CountReplicasAtOrAboveWithNotify(targetOffset)
}

func (e *Executor) requestReplicaAcknowledgements() error {
	if e.replicaPeers == nil {
		return nil
	}

	request := propagationFrame(&Request{Name: "REPLCONF", Args: [][]byte{[]byte("GETACK"), []byte("*")}})
	encoded, err := protocol.Encode(request)
	if err != nil {
		return fmt.Errorf("encode REPLCONF GETACK: %w", err)
	}
	if e.replication != nil {
		e.replication.AdvanceMasterOffset(int64(len(encoded)))
	}

	peers := e.replicaPeers.Snapshot()
	e.logger.Debug("requested replica acknowledgements", "replica_count", len(peers), "payload_size", len(encoded))
	for _, peer := range peers {
		if err := peer.WriteEncoded(encoded); err != nil {
			e.logger.Warn("failed to request replica ACK", "replica_id", peer.ID, "error", err)
			if closeErr := e.replicaPeers.RemoveAndClose(peer.ID); closeErr != nil {
				e.logger.Debug("failed to close replica after ACK request failure", "replica_id", peer.ID, "error", closeErr)
			}
		}
	}

	return nil
}

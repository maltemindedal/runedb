package command

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/server"
	"github.com/maltemindedal/runedb/internal/storage"
)

func TestExecutorExecute(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*testing.T, *Executor)
		request protocol.Value
		wait    time.Duration
		assert  func(*testing.T, protocol.Value, error)
	}{
		{
			name:    "PING returns PONG",
			request: requestValue("PING"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.SimpleString{Value: "PONG"})
			},
		},
		{
			name:    "PING with message echoes as bulk string",
			request: requestValue("PING", "hello"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("hello")})
			},
		},
		{
			name:    "ECHO returns payload",
			request: requestValue("ECHO", "hello"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("hello")})
			},
		},
		{
			name: "GET returns stored value",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "name", "RuneDB")); err != nil {
					t.Fatalf("SET error = %v", err)
				}
			},
			request: requestValue("GET", "name"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("RuneDB")})
			},
		},
		{
			name: "GET returns null after PX expiration",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "temp", "1", "PX", "10")); err != nil {
					t.Fatalf("SET error = %v", err)
				}
			},
			request: requestValue("GET", "temp"),
			wait:    20 * time.Millisecond,
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Null: true})
			},
		},
		{
			name: "GET rejects list values with wrong type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("RPUSH", "queue", "job-1")); err != nil {
					t.Fatalf("RPUSH error = %v", err)
				}
			},
			request: requestValue("GET", "queue"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name: "DEL removes existing keys and returns count",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "one", "1")); err != nil {
					t.Fatalf("SET one error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("SET", "two", "2")); err != nil {
					t.Fatalf("SET two error = %v", err)
				}
			},
			request: requestValue("DEL", "one", "missing", "two"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 2})
			},
		},
		{
			name: "DEL ignores expired keys",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				executor.store.Set("expired", []byte("gone"), time.Now().Add(-time.Millisecond).UnixMilli())
			},
			request: requestValue("DEL", "expired"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name: "LPUSH and LRANGE return list contents",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("LPUSH", "letters", "a", "b")); err != nil {
					t.Fatalf("LPUSH error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("RPUSH", "letters", "c")); err != nil {
					t.Fatalf("RPUSH error = %v", err)
				}
			},
			request: requestValue("LRANGE", "letters", "0", "-1"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("b")},
					protocol.BulkString{Data: []byte("a")},
					protocol.BulkString{Data: []byte("c")},
				}})
			},
		},
		{
			name: "ZADD and ZRANGE WITHSCORES return sorted contents",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("ZADD", "leaders", "2", "beta", "1", "alpha", "2", "aardvark")); err != nil {
					t.Fatalf("ZADD error = %v", err)
				}
			},
			request: requestValue("ZRANGE", "leaders", "0", "-1", "WITHSCORES"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("alpha")},
					protocol.BulkString{Data: []byte("1")},
					protocol.BulkString{Data: []byte("aardvark")},
					protocol.BulkString{Data: []byte("2")},
					protocol.BulkString{Data: []byte("beta")},
					protocol.BulkString{Data: []byte("2")},
				}})
			},
		},
		{
			name: "ZADD returns count of newly added members only",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("ZADD", "leaders", "1", "alpha")); err != nil {
					t.Fatalf("initial ZADD error = %v", err)
				}
			},
			request: requestValue("ZADD", "leaders", "2", "alpha", "3", "beta"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 1})
			},
		},
		{
			name:    "ZADD rejects invalid score",
			request: requestValue("ZADD", "leaders", "oops", "alpha"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotFloat) {
					t.Fatalf("Execute() error = %v, want ErrValueNotFloat", err)
				}
			},
		},
		{
			name: "XADD and XREAD return stream entries",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("XADD", "events", "1-0", "type", "start")); err != nil {
					t.Fatalf("first XADD error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("XADD", "events", "2-0", "type", "finish", "user", "42")); err != nil {
					t.Fatalf("second XADD error = %v", err)
				}
			},
			request: requestValue("XREAD", "STREAMS", "events", "0-0"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
					protocol.Array{Elements: []protocol.Value{
						protocol.BulkString{Data: []byte("events")},
						protocol.Array{Elements: []protocol.Value{
							protocol.Array{Elements: []protocol.Value{
								protocol.BulkString{Data: []byte("1-0")},
								protocol.Array{Elements: []protocol.Value{
									protocol.BulkString{Data: []byte("type")},
									protocol.BulkString{Data: []byte("start")},
								}},
							}},
							protocol.Array{Elements: []protocol.Value{
								protocol.BulkString{Data: []byte("2-0")},
								protocol.Array{Elements: []protocol.Value{
									protocol.BulkString{Data: []byte("type")},
									protocol.BulkString{Data: []byte("finish")},
									protocol.BulkString{Data: []byte("user")},
									protocol.BulkString{Data: []byte("42")},
								}},
							}},
						}},
					}},
				}})
			},
		},
		{
			name:    "XREAD returns empty array for missing stream",
			request: requestValue("XREAD", "STREAMS", "missing", "0-0"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{}})
			},
		},
		{
			name:    "XADD rejects malformed IDs",
			request: requestValue("XADD", "events", "bad-id", "field", "value"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrInvalidStreamID) {
					t.Fatalf("Execute() error = %v, want ErrInvalidStreamID", err)
				}
			},
		},
		{
			name: "XADD rejects non monotonic explicit IDs",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("XADD", "events", "1-0", "field", "value")); err != nil {
					t.Fatalf("initial XADD error = %v", err)
				}
			},
			request: requestValue("XADD", "events", "1-0", "field", "value"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrStreamIDTooSmall) {
					t.Fatalf("Execute() error = %v, want ErrStreamIDTooSmall", err)
				}
			},
		},
		{
			name: "XREAD rejects wrong value type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				executor.store.Set("events", []byte("plain"), 0)
			},
			request: requestValue("XREAD", "STREAMS", "events", "0-0"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name:    "XREAD rejects invalid syntax",
			request: requestValue("XREAD", "COUNT", "2", "STREAMS", "events", "0-0"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrSyntax) {
					t.Fatalf("Execute() error = %v, want ErrSyntax", err)
				}
			},
		},
		{
			name:    "ZRANGE rejects invalid trailing option",
			request: requestValue("ZRANGE", "leaders", "0", "-1", "REV"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrSyntax) {
					t.Fatalf("Execute() error = %v, want ErrSyntax", err)
				}
			},
		},
		{
			name: "LPUSH rejects wrong value type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				executor.store.Set("letters", []byte("hello"), 0)
			},
			request: requestValue("LPUSH", "letters", "a"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name:    "INCR initializes missing key",
			request: requestValue("INCR", "counter"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 1})
			},
		},
		{
			name: "INCR increments existing integer",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				executor.store.Set("counter", []byte("41"), 0)
			},
			request: requestValue("INCR", "counter"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 42})
			},
		},
		{
			name: "INCR rejects non-integer strings",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				executor.store.Set("counter", []byte("hello"), 0)
			},
			request: requestValue("INCR", "counter"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("Execute() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name:    "unknown command returns error",
			request: requestValue("NOPE"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("Execute() error = nil, want unknown command error")
				}
				if got := err.Error(); got != "unknown command \"NOPE\"" {
					t.Fatalf("error = %q, want %q", got, "unknown command \"NOPE\"")
				}
			},
		},
		{
			name:    "SET with invalid PX returns sentinel error",
			request: requestValue("SET", "temp", "1", "PX", "0"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrInvalidExpireTime) {
					t.Fatalf("Execute() error = %v, want ErrInvalidExpireTime", err)
				}
			},
		},
		{
			name:    "SET with invalid option returns syntax sentinel",
			request: requestValue("SET", "temp", "1", "NX", "10"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrSyntax) {
					t.Fatalf("Execute() error = %v, want ErrSyntax", err)
				}
			},
		},
		{
			name:    "ECHO wrong number of arguments returns error",
			request: requestValue("ECHO"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("Execute() error = nil, want wrong-argument-count error")
				}
				if got := err.Error(); got != "wrong number of arguments for 'ECHO' command" {
					t.Fatalf("error = %q, want %q", got, "wrong number of arguments for 'ECHO' command")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			executor := newTestExecutor()
			if tt.setup != nil {
				tt.setup(t, executor)
			}
			if tt.wait > 0 {
				time.Sleep(tt.wait)
			}

			value, err := executor.Execute(context.Background(), tt.request)
			tt.assert(t, value, err)
		})
	}
}

func TestExecutorXAddAutoGeneratesIDs(t *testing.T) {
	executor := newTestExecutor()

	first, err := executor.Execute(context.Background(), requestValue("XADD", "events", "*", "field", "one"))
	if err != nil {
		t.Fatalf("first XADD error = %v", err)
	}
	second, err := executor.Execute(context.Background(), requestValue("XADD", "events", "*", "field", "two"))
	if err != nil {
		t.Fatalf("second XADD error = %v", err)
	}

	firstIDText := mustBulkStringText(t, first)
	secondIDText := mustBulkStringText(t, second)

	firstID, err := storageParseStreamIDForTest(firstIDText)
	if err != nil {
		t.Fatalf("parse first ID error = %v", err)
	}
	secondID, err := storageParseStreamIDForTest(secondIDText)
	if err != nil {
		t.Fatalf("parse second ID error = %v", err)
	}
	if secondID.milliseconds < firstID.milliseconds || (secondID.milliseconds == firstID.milliseconds && secondID.sequence <= firstID.sequence) {
		t.Fatalf("second auto-generated ID %q is not greater than first ID %q", secondIDText, firstIDText)
	}
}

func storageParseStreamIDForTest(raw string) (struct{ milliseconds, sequence int64 }, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return struct{ milliseconds, sequence int64 }{}, errors.New("invalid stream ID")
	}
	milliseconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return struct{ milliseconds, sequence int64 }{}, err
	}
	sequence, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return struct{ milliseconds, sequence int64 }{}, err
	}
	return struct{ milliseconds, sequence int64 }{milliseconds: milliseconds, sequence: sequence}, nil
}

func TestExecutorBLPop(t *testing.T) {
	executor := newTestExecutor()

	pushErrCh := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, err := executor.Execute(context.Background(), requestValue("RPUSH", "jobs", "build"))
		pushErrCh <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	value, err := executor.Execute(ctx, requestValue("BLPOP", "jobs"))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if pushErr := <-pushErrCh; pushErr != nil {
		t.Fatalf("RPUSH error = %v", pushErr)
	}

	assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("jobs")},
		protocol.BulkString{Data: []byte("build")},
	}})
}

func TestExecutorTransactions(t *testing.T) {
	t.Run("MULTI queues commands until EXEC", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		value, err := executor.Execute(ctx, requestValue("MULTI"))
		if err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})

		value, err = executor.Execute(ctx, requestValue("SET", "name", "RuneDB"))
		if err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		value, err = executor.Execute(ctx, requestValue("GET", "name"))
		if err != nil {
			t.Fatalf("queued GET error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		value, err = executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.SimpleString{Value: "OK"},
			protocol.BulkString{Data: []byte("RuneDB")},
		}})
	})

	t.Run("EXEC includes per-command errors", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "bad", "hello")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("INCR", "bad")); err != nil {
			t.Fatalf("queued INCR error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.SimpleString{Value: "OK"},
			protocol.ErrorValue{Message: "ERR value is not an integer or out of range"},
		}})
	})

	t.Run("transaction state errors use Redis-compatible sentinels", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecWithoutMulti) {
			t.Fatalf("EXEC error = %v, want ErrExecWithoutMulti", err)
		}
		if _, err := executor.Execute(ctx, requestValue("DISCARD")); !errors.Is(err, ErrDiscardWithoutMulti) {
			t.Fatalf("DISCARD error = %v, want ErrDiscardWithoutMulti", err)
		}
		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("first MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("MULTI")); !errors.Is(err, ErrMultiNested) {
			t.Fatalf("nested MULTI error = %v, want ErrMultiNested", err)
		}
	})

	t.Run("WATCH aborts EXEC after another client modifies a watched key", func(t *testing.T) {
		executor := newTestExecutor()
		watcherCtx := withClientStateForExecutor(context.Background(), executor, 1)
		writerCtx := withClientStateForExecutor(context.Background(), executor, 2)

		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "counter")); err != nil {
			t.Fatalf("WATCH error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("SET", "counter", "2")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("SET", "counter", "1")); err != nil {
			t.Fatalf("writer SET error = %v", err)
		}

		value, err := executor.Execute(watcherCtx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Null: true})

		value, err = executor.Execute(watcherCtx, requestValue("GET", "counter"))
		if err != nil {
			t.Fatalf("GET after aborted EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("1")})
	})

	t.Run("WATCH is rejected inside MULTI", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("WATCH", "counter")); !errors.Is(err, ErrWatchInsideMulti) {
			t.Fatalf("WATCH error = %v, want ErrWatchInsideMulti", err)
		}
	})

	t.Run("queue-time validation errors abort EXEC", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("NOPE")); err == nil || err.Error() != "unknown command \"NOPE\"" {
			t.Fatalf("unknown command error = %v, want unknown command \"NOPE\"", err)
		}
		value, err := executor.Execute(ctx, requestValue("SET", "name", "RuneDB"))
		if err != nil {
			t.Fatalf("queued SET after invalid command error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
			t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
		}

		value, err = executor.Execute(ctx, requestValue("GET", "name"))
		if err != nil {
			t.Fatalf("GET after EXECABORT error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})
	})

	t.Run("queue-time syntax errors are returned immediately", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "temp", "1", "NX", "10")); !errors.Is(err, ErrSyntax) {
			t.Fatalf("queued SET syntax error = %v, want ErrSyntax", err)
		}
		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
			t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
		}
	})

	t.Run("queue-time malformed stream IDs abort EXEC", func(t *testing.T) {
		tests := []struct {
			name    string
			request protocol.Value
		}{
			{
				name:    "XADD malformed ID",
				request: requestValue("XADD", "events", "bad-id", "field", "value"),
			},
			{
				name:    "XREAD malformed ID",
				request: requestValue("XREAD", "STREAMS", "events", "bad-id"),
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				executor := newTestExecutor()
				ctx := withClientStateForExecutor(context.Background(), executor, 1)

				if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
					t.Fatalf("MULTI error = %v", err)
				}
				if _, err := executor.Execute(ctx, tt.request); !errors.Is(err, ErrInvalidStreamID) {
					t.Fatalf("queued stream validation error = %v, want ErrInvalidStreamID", err)
				}
				if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
					t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
				}
			})
		}
	})
}

func TestDecodeRequest(t *testing.T) {
	tests := []struct {
		name   string
		value  protocol.Value
		want   *Request
		assert func(*testing.T, error)
	}{
		{
			name:  "bulk string command",
			value: requestValue("SET", "name", "RuneDB"),
			want:  &Request{Name: "SET", Args: [][]byte{[]byte("name"), []byte("RuneDB")}},
			assert: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("DecodeRequest() error = %v", err)
				}
			},
		},
		{
			name: "mixed simple string and integer tokens",
			value: protocol.Array{Elements: []protocol.Value{
				protocol.SimpleString{Value: "PING"},
				protocol.Integer{Value: 42},
			}},
			want: &Request{Name: "PING", Args: [][]byte{[]byte("42")}},
			assert: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("DecodeRequest() error = %v", err)
				}
			},
		},
		{
			name:  "null array rejected",
			value: protocol.Array{Null: true},
			assert: func(t *testing.T, err error) {
				t.Helper()
				var protocolErr ProtocolError
				if !errors.As(err, &protocolErr) {
					t.Fatalf("DecodeRequest() error = %v, want ProtocolError", err)
				}
			},
		},
		{
			name: "null bulk string token rejected",
			value: protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: []byte("GET")},
				protocol.BulkString{Null: true},
			}},
			assert: func(t *testing.T, err error) {
				t.Helper()
				var protocolErr ProtocolError
				if !errors.As(err, &protocolErr) {
					t.Fatalf("DecodeRequest() error = %v, want ProtocolError", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			request, err := DecodeRequest(tt.value)
			tt.assert(t, err)
			if tt.want == nil || err != nil {
				return
			}

			if request.Name != tt.want.Name {
				t.Fatalf("request.Name = %q, want %q", request.Name, tt.want.Name)
			}
			if len(request.Args) != len(tt.want.Args) {
				t.Fatalf("len(request.Args) = %d, want %d", len(request.Args), len(tt.want.Args))
			}
			for i := range request.Args {
				if string(request.Args[i]) != string(tt.want.Args[i]) {
					t.Fatalf("request.Args[%d] = %q, want %q", i, string(request.Args[i]), string(tt.want.Args[i]))
				}
			}
		})
	}
}

func newTestExecutor() *Executor {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewExecutor(storage.NewStore(), logger)
}

func withClientStateForExecutor(ctx context.Context, executor *Executor, id uint64) context.Context {
	state := &server.ClientState{ID: id, Authenticated: true}
	state.SetWatchRegistry(executor.WatchRegistry())
	return server.WithClientState(ctx, state)
}

func requestValue(parts ...string) protocol.Value {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	return protocol.Array{Elements: elements}
}

func assertValueEqual(t *testing.T, got protocol.Value, want protocol.Value) {
	t.Helper()

	switch typedWant := want.(type) {
	case protocol.SimpleString:
		typedGot, ok := got.(protocol.SimpleString)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("simple string = %q, want %q", typedGot.Value, typedWant.Value)
		}
	case protocol.BulkString:
		gotText, gotNull, ok := bulkStringContent(got)
		if !ok {
			t.Fatalf("value type = %T, want bulk-string-compatible type", got)
		}
		if gotNull != typedWant.Null {
			t.Fatalf("bulk string null = %v, want %v", gotNull, typedWant.Null)
		}
		if gotText != string(typedWant.Data) {
			t.Fatalf("bulk string = %q, want %q", gotText, string(typedWant.Data))
		}
	case protocol.Integer:
		typedGot, ok := got.(protocol.Integer)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("integer = %d, want %d", typedGot.Value, typedWant.Value)
		}
	case protocol.Array:
		typedGot, ok := got.(protocol.Array)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null {
			t.Fatalf("array null = %v, want %v", typedGot.Null, typedWant.Null)
		}
		if len(typedGot.Elements) != len(typedWant.Elements) {
			t.Fatalf("len(array) = %d, want %d", len(typedGot.Elements), len(typedWant.Elements))
		}
		for i := range typedWant.Elements {
			assertValueEqual(t, typedGot.Elements[i], typedWant.Elements[i])
		}
	case protocol.ErrorValue:
		typedGot, ok := got.(protocol.ErrorValue)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Message != typedWant.Message {
			t.Fatalf("error message = %q, want %q", typedGot.Message, typedWant.Message)
		}
	default:
		t.Fatalf("unsupported wanted type %T", want)
	}
}

func mustBulkStringText(t *testing.T, value protocol.Value) string {
	t.Helper()

	text, isNull, ok := bulkStringContent(value)
	if !ok {
		t.Fatalf("value type = %T, want bulk-string-compatible type", value)
	}
	if isNull {
		t.Fatal("bulk string unexpectedly null")
	}

	return text
}

func bulkStringContent(value protocol.Value) (string, bool, bool) {
	switch typed := value.(type) {
	case protocol.BulkString:
		return string(typed.Data), typed.Null, true
	case protocol.TextBulkString:
		return typed.Value, typed.Null, true
	default:
		return "", false, false
	}
}

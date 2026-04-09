package command

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/maltemindedal/godis/internal/protocol"
	"github.com/maltemindedal/godis/internal/storage"
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
				if _, err := executor.Execute(context.Background(), requestValue("SET", "name", "godis")); err != nil {
					t.Fatalf("SET error = %v", err)
				}
			},
			request: requestValue("GET", "name"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("godis")})
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

func TestDecodeRequest(t *testing.T) {
	tests := []struct {
		name   string
		value  protocol.Value
		want   *Request
		assert func(*testing.T, error)
	}{
		{
			name:  "bulk string command",
			value: requestValue("SET", "name", "godis"),
			want:  &Request{Name: "SET", Args: [][]byte{[]byte("name"), []byte("godis")}},
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
		typedGot, ok := got.(protocol.BulkString)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null {
			t.Fatalf("bulk string null = %v, want %v", typedGot.Null, typedWant.Null)
		}
		if string(typedGot.Data) != string(typedWant.Data) {
			t.Fatalf("bulk string = %q, want %q", string(typedGot.Data), string(typedWant.Data))
		}
	case protocol.Integer:
		typedGot, ok := got.(protocol.Integer)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("integer = %d, want %d", typedGot.Value, typedWant.Value)
		}
	default:
		t.Fatalf("unsupported wanted type %T", want)
	}
}

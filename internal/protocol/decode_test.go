package protocol

import (
	"errors"
	"reflect"
	"testing"
)

func TestDecodeCompleteFrames(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{name: "simple string", input: "+OK\r\n", want: SimpleString{Value: "OK"}},
		{name: "error", input: "-ERR boom\r\n", want: ErrorValue{Message: "ERR boom"}},
		{name: "integer", input: ":42\r\n", want: Integer{Value: 42}},
		{name: "negative integer", input: ":-7\r\n", want: Integer{Value: -7}},
		{name: "bulk string", input: "$5\r\nhello\r\n", want: BulkString{Data: []byte("hello")}},
		{name: "empty bulk string", input: "$0\r\n\r\n", want: BulkString{Data: []byte{}}},
		{name: "null bulk string", input: "$-1\r\n", want: BulkString{Null: true}},
		{name: "boolean true", input: "#t\r\n", want: Boolean{Value: true}},
		{name: "boolean false", input: "#f\r\n", want: Boolean{Value: false}},
		{name: "null", input: "_\r\n", want: Null{}},
		{name: "null array", input: "*-1\r\n", want: Array{Null: true}},
		{name: "empty array", input: "*0\r\n", want: Array{Elements: []Value{}}},
		{
			name:  "array of bulk strings",
			input: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
			want: Array{Elements: []Value{
				BulkString{Data: []byte("ECHO")},
				BulkString{Data: []byte("hello")},
			}},
		},
		{
			name:  "nested array",
			input: "*2\r\n*1\r\n:1\r\n+done\r\n",
			want: Array{Elements: []Value{
				Array{Elements: []Value{Integer{Value: 1}}},
				SimpleString{Value: "done"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, consumed, err := Decode([]byte(tt.input))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if consumed != len(tt.input) {
				t.Fatalf("Decode() consumed = %d, want %d", consumed, len(tt.input))
			}
			if !reflect.DeepEqual(value, tt.want) {
				t.Fatalf("Decode() = %#v, want %#v", value, tt.want)
			}
		})
	}
}

func TestDecodeIncompleteAtEveryBoundary(t *testing.T) {
	frames := []string{
		"+OK\r\n",
		"-ERR boom\r\n",
		":42\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"#t\r\n",
		"_\r\n",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
		"*2\r\n*1\r\n:1\r\n+done\r\n",
	}

	for _, frame := range frames {
		for cut := 0; cut < len(frame); cut++ {
			value, consumed, err := Decode([]byte(frame[:cut]))
			if !errors.Is(err, ErrIncomplete) {
				t.Fatalf("Decode(%q) error = %v, want ErrIncomplete", frame[:cut], err)
			}
			if value != nil || consumed != 0 {
				t.Fatalf("Decode(%q) = (%#v, %d), want (nil, 0)", frame[:cut], value, consumed)
			}
		}
	}
}

func TestDecodeLeavesTrailingBytes(t *testing.T) {
	input := []byte("+first\r\n+second\r\n")

	value, consumed, err := Decode(input)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if want := (SimpleString{Value: "first"}); value != want {
		t.Fatalf("Decode() = %#v, want %#v", value, want)
	}
	if want := len("+first\r\n"); consumed != want {
		t.Fatalf("Decode() consumed = %d, want %d", consumed, want)
	}

	value, consumed, err = Decode(input[consumed:])
	if err != nil {
		t.Fatalf("Decode() second frame error = %v", err)
	}
	if want := (SimpleString{Value: "second"}); value != want {
		t.Fatalf("Decode() second frame = %#v, want %#v", value, want)
	}
	if want := len("+second\r\n"); consumed != want {
		t.Fatalf("Decode() second frame consumed = %d, want %d", consumed, want)
	}
}

func TestDecodeDoesNotRetainBuffer(t *testing.T) {
	input := []byte("$5\r\nhello\r\n")

	value, _, err := Decode(input)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	copy(input, "$5\r\nXXXXX\r\n")

	bulk, ok := value.(BulkString)
	if !ok {
		t.Fatalf("Decode() = %#v, want BulkString", value)
	}
	if got := string(bulk.Data); got != "hello" {
		t.Fatalf("bulk string payload = %q, want %q after buffer reuse", got, "hello")
	}
}

func TestDecodeHugeBulkLengthReportsIncomplete(t *testing.T) {
	// A near-MaxInt declared length must not overflow the payload-availability
	// check into a false "complete" read that then indexes out of range.
	for _, length := range []string{"9223372036854775807", "9223372036854775806"} {
		input := []byte("$" + length + "\r\n")
		value, consumed, err := Decode(input)
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Decode($%s) error = %v, want ErrIncomplete", length, err)
		}
		if value != nil || consumed != 0 {
			t.Fatalf("Decode($%s) = (%#v, %d), want (nil, 0)", length, value, consumed)
		}
	}
}

func TestDecodeDeeplyNestedArrayIsBounded(t *testing.T) {
	// Deep nesting must fail as a protocol error rather than recursing until
	// the goroutine stack overflows.
	var buf []byte
	for i := 0; i < maxNestingDepth+16; i++ {
		buf = append(buf, "*1\r\n"...)
	}
	buf = append(buf, ":1\r\n"...)

	_, consumed, err := Decode(buf)
	if err == nil || errors.Is(err, ErrIncomplete) {
		t.Fatalf("Decode(deeply nested) error = %v, want permanent protocol error", err)
	}
	if consumed != 0 {
		t.Fatalf("Decode(deeply nested) consumed = %d, want 0", consumed)
	}
}

func TestDecodeNestingAtLimitSucceeds(t *testing.T) {
	// One level below the guard must still decode so the limit does not reject
	// legitimately nested frames.
	depth := maxNestingDepth - 1
	var buf []byte
	for i := 0; i < depth; i++ {
		buf = append(buf, "*1\r\n"...)
	}
	buf = append(buf, ":1\r\n"...)

	value, consumed, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode(nested to limit) error = %v", err)
	}
	if consumed != len(buf) {
		t.Fatalf("Decode(nested to limit) consumed = %d, want %d", consumed, len(buf))
	}
	for i := 0; i < depth; i++ {
		array, ok := value.(Array)
		if !ok || len(array.Elements) != 1 {
			t.Fatalf("level %d = %#v, want single-element array", i, value)
		}
		value = array.Elements[0]
	}
	if want := (Integer{Value: 1}); value != want {
		t.Fatalf("innermost value = %#v, want %#v", value, want)
	}
}

func TestDecodeProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "unsupported prefix", input: "X hello\r\n"},
		{name: "line missing carriage return", input: "+OK\n"},
		{name: "non-numeric integer", input: ":abc\r\n"},
		{name: "invalid boolean marker", input: "#x\r\n"},
		{name: "non-empty null payload", input: "_x\r\n"},
		{name: "invalid bulk string length", input: "$-2\r\n"},
		{name: "bulk string missing terminator", input: "$5\r\nhelloXX"},
		{name: "invalid array length", input: "*-2\r\n"},
		{name: "invalid array element", input: "*1\r\nX\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, consumed, err := Decode([]byte(tt.input))
			if err == nil || errors.Is(err, ErrIncomplete) {
				t.Fatalf("Decode(%q) error = %v, want permanent protocol error", tt.input, err)
			}
			if consumed != 0 {
				t.Fatalf("Decode(%q) consumed = %d, want 0", tt.input, consumed)
			}
		})
	}
}

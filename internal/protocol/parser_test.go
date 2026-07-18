package protocol

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

type chunkReader struct {
	chunks [][]byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}

	chunk := r.chunks[0]
	n := copy(p, chunk)
	if n == len(chunk) {
		r.chunks = r.chunks[1:]
	} else {
		r.chunks[0] = chunk[n:]
	}

	return n, nil
}

func TestParserParse(t *testing.T) {
	tests := []struct {
		name   string
		reader func() io.Reader
		assert func(*testing.T, Value, error)
	}{
		{
			name:   "array of bulk strings",
			reader: func() io.Reader { return stringsReader("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n") },
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				array, ok := value.(Array)
				if !ok {
					t.Fatalf("Parse() type = %T, want Array", value)
				}
				if len(array.Elements) != 2 {
					t.Fatalf("len(array.Elements) = %d, want 2", len(array.Elements))
				}
				assertBulkString(t, array.Elements[0], "ECHO")
				assertBulkString(t, array.Elements[1], "hello")
			},
		},
		{
			name: "fragmented input",
			reader: func() io.Reader {
				return &chunkReader{chunks: [][]byte{
					[]byte("*2\r\n$4\r\nEC"),
					[]byte("HO\r\n$5\r\nhe"),
					[]byte("llo\r\n"),
				}}
			},
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				array := value.(Array)
				assertBulkString(t, array.Elements[0], "ECHO")
				assertBulkString(t, array.Elements[1], "hello")
			},
		},
		{
			name:   "malformed bulk string",
			reader: func() io.Reader { return stringsReader("$5\r\nhell\r\n") },
			assert: func(t *testing.T, _ Value, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("Parse() error = nil, want malformed bulk string error")
				}
			},
		},
		{
			name:   "RESP3 null placeholder",
			reader: func() io.Reader { return stringsReader("_\r\n") },
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				if _, ok := value.(Null); !ok {
					t.Fatalf("Parse() type = %T, want Null", value)
				}
			},
		},
		{
			name: "long line over buffered reader capacity",
			reader: func() io.Reader {
				return bufio.NewReaderSize(strings.NewReader("+"+strings.Repeat("a", 64)+"\r\n"), 8)
			},
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				simple, ok := value.(SimpleString)
				if !ok {
					t.Fatalf("Parse() type = %T, want SimpleString", value)
				}
				if simple.Value != strings.Repeat("a", 64) {
					t.Fatalf("simple string = %q, want %q", simple.Value, strings.Repeat("a", 64))
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.reader())
			value, err := parser.Parse()
			tt.assert(t, value, err)
		})
	}
}

// TestParserRejectsOversizedFrames verifies that forged length prefixes and
// terminator-less lines are rejected with a permanent protocol error rather
// than triggering a giant allocation (which for a near-MaxInt length would
// panic and, unrecovered, crash the whole process).
func TestParserRejectsOversizedFrames(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "bulk length near MaxInt does not allocate",
			input: "$9223372036854775807\r\n",
		},
		{
			name:  "bulk length just over cap",
			input: "$536870913\r\n", // maxBulkStringLength + 1
		},
		{
			name:  "array count near MaxInt does not allocate",
			input: "*9223372036854775807\r\n",
		},
		{
			name:  "array count just over cap",
			input: "*1048577\r\n", // maxArrayElements + 1
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(stringsReader(tt.input))
			if _, err := parser.Parse(); err == nil {
				t.Fatalf("Parse(%q) error = nil, want oversized-frame rejection", tt.input)
			}
		})
	}
}

// TestParserRejectsUnboundedLine verifies that a line which never sends its
// terminator is rejected once it passes maxLineLength instead of growing the
// line buffer without bound.
func TestParserRejectsUnboundedLine(t *testing.T) {
	input := "+" + strings.Repeat("a", maxLineLength*2) // no trailing CRLF
	parser := NewParser(strings.NewReader(input))
	if _, err := parser.Parse(); err == nil {
		t.Fatal("Parse() error = nil, want unbounded-line rejection")
	}
}

func TestNewParserReusesBufferedReader(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte("+OK\r\n")))
	parser := NewParser(reader)

	if parser.reader != reader {
		t.Fatal("NewParser() did not reuse the provided buffered reader")
	}
}

func stringsReader(value string) io.Reader {
	return &chunkReader{chunks: [][]byte{[]byte(value)}}
}

func assertBulkString(t *testing.T, value Value, want string) {
	t.Helper()

	bulk, ok := value.(BulkString)
	if !ok {
		t.Fatalf("value type = %T, want BulkString", value)
	}
	if bulk.Null {
		t.Fatal("bulk string unexpectedly null")
	}
	if string(bulk.Data) != want {
		t.Fatalf("bulk string = %q, want %q", string(bulk.Data), want)
	}
}

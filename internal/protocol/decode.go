package protocol

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrIncomplete reports that a buffer does not yet contain a complete RESP
// frame. Callers should retry Decode after more bytes arrive.
var ErrIncomplete = errors.New("protocol: incomplete frame")

// maxNestingDepth bounds RESP array nesting so a maliciously deep frame cannot
// exhaust the goroutine stack through Decode's recursion.
const maxNestingDepth = 128

// Decode parses one RESP value from the front of buf without blocking.
// It returns the parsed value and the number of bytes consumed. When buf does
// not yet hold a complete frame, Decode returns ErrIncomplete and consumes
// nothing. Any other error is a permanent protocol error. Returned values do
// not retain buf, so callers may reuse or compact it after Decode returns.
func Decode(buf []byte) (Value, int, error) {
	return decode(buf, 0)
}

func decode(buf []byte, depth int) (Value, int, error) {
	if len(buf) == 0 {
		return nil, 0, ErrIncomplete
	}

	switch prefix := buf[0]; prefix {
	case '+':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		return SimpleString{Value: string(line)}, 1 + n, nil
	case '-':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		return ErrorValue{Message: string(line)}, 1 + n, nil
	case ':':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		value, err := parseIntBytes(line, 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("protocol: parse integer: %w", err)
		}
		return Integer{Value: value}, 1 + n, nil
	case '#':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		switch string(line) {
		case "t":
			return Boolean{Value: true}, 1 + n, nil
		case "f":
			return Boolean{Value: false}, 1 + n, nil
		default:
			return nil, 0, fmt.Errorf("protocol: invalid boolean marker %q", string(line))
		}
	case '_':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		if len(line) != 0 {
			return nil, 0, fmt.Errorf("protocol: invalid null payload %q", string(line))
		}
		return Null{}, 1 + n, nil
	case '$':
		return decodeBulkString(buf)
	case '*':
		return decodeArray(buf, depth)
	default:
		return nil, 0, fmt.Errorf("protocol: unsupported frame prefix %q", string(prefix))
	}
}

func decodeBulkString(buf []byte) (Value, int, error) {
	line, n, err := decodeLine(buf[1:])
	if err != nil {
		return nil, 0, err
	}
	consumed := 1 + n

	length, err := parseDecimalInt(line)
	if err != nil {
		return nil, 0, fmt.Errorf("protocol: parse bulk string length: %w", err)
	}
	if length == -1 {
		return BulkString{Null: true}, consumed, nil
	}
	if length < -1 {
		return nil, 0, fmt.Errorf("protocol: invalid bulk string length %d", length)
	}

	remaining := buf[consumed:]
	if length > len(remaining)-2 {
		return nil, 0, ErrIncomplete
	}
	if remaining[length] != '\r' || remaining[length+1] != '\n' {
		return nil, 0, fmt.Errorf("protocol: bulk string payload missing CRLF terminator")
	}

	return BulkString{Data: bytes.Clone(remaining[:length])}, consumed + length + 2, nil
}

func decodeArray(buf []byte, depth int) (Value, int, error) {
	if depth >= maxNestingDepth {
		return nil, 0, fmt.Errorf("protocol: array nesting exceeds %d levels", maxNestingDepth)
	}

	line, n, err := decodeLine(buf[1:])
	if err != nil {
		return nil, 0, err
	}
	consumed := 1 + n

	count, err := parseDecimalInt(line)
	if err != nil {
		return nil, 0, fmt.Errorf("protocol: parse array length: %w", err)
	}
	if count == -1 {
		return Array{Null: true}, consumed, nil
	}
	if count < -1 {
		return nil, 0, fmt.Errorf("protocol: invalid array length %d", count)
	}

	elements := make([]Value, 0, min(count, 64))
	for i := 0; i < count; i++ {
		element, elementN, err := decode(buf[consumed:], depth+1)
		if err != nil {
			if errors.Is(err, ErrIncomplete) {
				return nil, 0, ErrIncomplete
			}
			return nil, 0, fmt.Errorf("protocol: parse array element %d: %w", i, err)
		}
		elements = append(elements, element)
		consumed += elementN
	}

	return Array{Elements: elements}, consumed, nil
}

// decodeLine locates a CRLF-terminated line at the front of buf and returns
// the line content and the number of bytes consumed including the terminator.
func decodeLine(buf []byte) ([]byte, int, error) {
	idx := bytes.IndexByte(buf, '\n')
	if idx < 0 {
		return nil, 0, ErrIncomplete
	}
	if idx == 0 || buf[idx-1] != '\r' {
		return nil, 0, fmt.Errorf("protocol: line missing CRLF terminator")
	}

	return buf[:idx-1], idx + 1, nil
}

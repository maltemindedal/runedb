package protocol

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrIncomplete reports that a buffer does not yet contain a complete RESP
// frame. Callers should retry Decode after more bytes arrive.
var ErrIncomplete = errors.New("protocol: incomplete frame")

// ErrMissingCRLF reports a line or bulk payload that is not terminated by CRLF.
// It is a typed sentinel so callers can match it with errors.Is rather than the
// message text. AOF replay relies on this to tell a torn trailing record (a
// recoverable truncated tail) apart from genuine corruption.
var ErrMissingCRLF = errors.New("protocol: line missing CRLF terminator")

// IncompleteError reports an incomplete frame along with Need, a lower bound on
// the total buffer length the frame requires. Callers can defer re-decoding
// until the buffer reaches Need bytes to avoid rescanning the same prefix on
// every append. Need is only populated when the shortfall is known from a
// length prefix; a line still missing its terminator reports the bare
// ErrIncomplete sentinel instead. errors.Is(err, ErrIncomplete) matches both.
type IncompleteError struct {
	Need int
}

// Error reports the same message as the bare ErrIncomplete sentinel, so the
// byte hint stays an implementation detail of the error value.
func (e *IncompleteError) Error() string { return ErrIncomplete.Error() }

// Unwrap returns ErrIncomplete so errors.Is matches both error shapes.
func (e *IncompleteError) Unwrap() error { return ErrIncomplete }

// incompleteNeed returns an incomplete-frame error carrying a total-bytes hint,
// or the bare sentinel when the hint is non-positive.
func incompleteNeed(total int) error {
	if total <= 0 {
		return ErrIncomplete
	}
	return &IncompleteError{Need: total}
}

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
		value, err := integerFromLine(line)
		if err != nil {
			return nil, 0, err
		}
		return value, 1 + n, nil
	case '#':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		value, err := booleanFromMarker(line)
		if err != nil {
			return nil, 0, err
		}
		return value, 1 + n, nil
	case '_':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		value, err := nullFromPayload(line)
		if err != nil {
			return nil, 0, err
		}
		return value, 1 + n, nil
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
		return nil, 0, incompleteNeed(consumed + length + 2)
	}
	if remaining[length] != '\r' || remaining[length+1] != '\n' {
		return nil, 0, fmt.Errorf("protocol: bulk string payload missing CRLF terminator: %w", ErrMissingCRLF)
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
				var incomplete *IncompleteError
				if errors.As(err, &incomplete) {
					return nil, 0, incompleteNeed(consumed + incomplete.Need)
				}
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

	content, err := trimCRLF(buf[:idx+1])
	if err != nil {
		return nil, 0, err
	}

	return content, idx + 1, nil
}

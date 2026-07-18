package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

// Parser reads RESP values from an io.Reader.
type Parser struct {
	reader  *bufio.Reader
	lineBuf []byte
}

const (
	// maxBulkStringLength bounds a single bulk string payload the streaming
	// Parser will allocate. It mirrors the event loop's read-buffer limit
	// (the equivalent of Redis' proto-max-bulk-len) so a forged length prefix
	// cannot trigger a huge make([]byte, ...) that panics or OOMs the process.
	maxBulkStringLength = 512 * 1024 * 1024
	// maxArrayElements bounds the declared element count of a single RESP array
	// so a forged multibulk header cannot pre-allocate an enormous slice.
	maxArrayElements = 1024 * 1024
	// maxLineLength bounds a single CRLF-terminated line (protocol headers and
	// length prefixes) so a stream that never sends a terminator cannot grow
	// the line buffer without bound.
	maxLineLength = 64 * 1024
)

// NewParser constructs a Parser backed by a bufio.Reader.
func NewParser(reader io.Reader) *Parser {
	if buffered, ok := reader.(*bufio.Reader); ok {
		return &Parser{reader: buffered}
	}

	return &Parser{reader: bufio.NewReader(reader)}
}

// Parse reads the next RESP value from the underlying reader.
func (p *Parser) Parse() (Value, error) {
	prefix, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+':
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		return SimpleString{Value: line}, nil
	case '-':
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		return ErrorValue{Message: line}, nil
	case ':':
		line, err := p.readLineBytes()
		if err != nil {
			return nil, err
		}
		return integerFromLine(line)
	case '$':
		return p.parseBulkString()
	case '*':
		return p.parseArray()
	case '#':
		line, err := p.readLineBytes()
		if err != nil {
			return nil, err
		}
		return booleanFromMarker(line)
	case '_':
		line, err := p.readLineBytes()
		if err != nil {
			return nil, err
		}
		return nullFromPayload(line)
	default:
		return nil, fmt.Errorf("protocol: unsupported frame prefix %q", string(prefix))
	}
}

func (p *Parser) parseBulkString() (Value, error) {
	line, err := p.readLineBytes()
	if err != nil {
		return nil, err
	}

	length, err := parseDecimalInt(line)
	if err != nil {
		return nil, fmt.Errorf("protocol: parse bulk string length: %w", err)
	}
	if length == -1 {
		return BulkString{Null: true}, nil
	}
	if length < -1 {
		return nil, fmt.Errorf("protocol: invalid bulk string length %d", length)
	}
	if length > maxBulkStringLength {
		return nil, fmt.Errorf("protocol: bulk string length %d exceeds %d byte limit", length, maxBulkStringLength)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(p.reader, payload); err != nil {
		return nil, fmt.Errorf("protocol: read bulk string payload: %w", err)
	}
	if err := p.expectCRLF(); err != nil {
		return nil, fmt.Errorf("protocol: bulk string payload missing CRLF terminator: %w", err)
	}

	return BulkString{Data: payload}, nil
}

func (p *Parser) parseArray() (Value, error) {
	line, err := p.readLineBytes()
	if err != nil {
		return nil, err
	}

	count, err := parseDecimalInt(line)
	if err != nil {
		return nil, fmt.Errorf("protocol: parse array length: %w", err)
	}
	if count == -1 {
		return Array{Null: true}, nil
	}
	if count < -1 {
		return nil, fmt.Errorf("protocol: invalid array length %d", count)
	}
	if count > maxArrayElements {
		return nil, fmt.Errorf("protocol: array length %d exceeds %d element limit", count, maxArrayElements)
	}

	elements := make([]Value, 0, min(count, 64))
	for i := 0; i < count; i++ {
		element, err := p.Parse()
		if err != nil {
			return nil, fmt.Errorf("protocol: parse array element %d: %w", i, err)
		}
		elements = append(elements, element)
	}

	return Array{Elements: elements}, nil
}

func (p *Parser) readLine() (string, error) {
	line, err := p.readLineBytes()
	if err != nil {
		return "", err
	}

	return string(line), nil
}

func (p *Parser) readLineBytes() ([]byte, error) {
	line, err := p.reader.ReadSlice('\n')
	if err == nil {
		return trimCRLF(line)
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		return nil, err
	}

	combined := append(p.lineBuf[:0], line...)
	for {
		fragment, readErr := p.reader.ReadSlice('\n')
		combined = append(combined, fragment...)
		if readErr == nil {
			p.lineBuf = combined[:0]
			return trimCRLF(combined)
		}
		if !errors.Is(readErr, bufio.ErrBufferFull) {
			p.lineBuf = combined[:0]
			return nil, readErr
		}
		if len(combined) > maxLineLength {
			p.lineBuf = combined[:0]
			return nil, fmt.Errorf("protocol: line exceeds %d byte limit", maxLineLength)
		}
	}
}

func (p *Parser) expectCRLF() error {
	carriage, err := p.reader.ReadByte()
	if err != nil {
		return err
	}
	newline, err := p.reader.ReadByte()
	if err != nil {
		return err
	}
	if carriage != '\r' || newline != '\n' {
		return ErrMissingCRLF
	}

	return nil
}

func trimCRLF(line []byte) ([]byte, error) {
	if len(line) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return nil, ErrMissingCRLF
	}

	return line[:len(line)-2], nil
}

func parseDecimalInt(raw []byte) (int, error) {
	value, err := parseIntBytes(raw, 10, 64)
	if err != nil {
		return 0, err
	}

	maxInt := int64(math.MaxInt)
	minInt := int64(math.MinInt)
	if value < minInt || value > maxInt {
		return 0, fmt.Errorf("value %d out of int range", value)
	}

	return int(value), nil
}

func parseIntBytes(raw []byte, base, bitSize int) (int64, error) {
	if len(raw) == 0 {
		return 0, strconv.ErrSyntax
	}
	if base != 10 {
		return 0, fmt.Errorf("unsupported base %d", base)
	}
	if bitSize <= 0 || bitSize > 64 {
		return 0, fmt.Errorf("unsupported bit size %d", bitSize)
	}

	negative := false
	if raw[0] == '-' || raw[0] == '+' {
		negative = raw[0] == '-'
		raw = raw[1:]
		if len(raw) == 0 {
			return 0, strconv.ErrSyntax
		}
	}

	maxAbs := uint64(1) << (bitSize - 1)
	maxPositive := maxAbs - 1
	limit := maxPositive
	if negative {
		limit = maxAbs
	}
	cutoff := limit / uint64(base)
	cutlim := limit % uint64(base)

	var value uint64
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, strconv.ErrSyntax
		}
		parsed := uint64(digit - '0')
		if value > cutoff || (value == cutoff && parsed > cutlim) {
			return 0, strconv.ErrRange
		}
		value = value*uint64(base) + parsed
	}

	if negative {
		if bitSize == 64 && value == maxAbs {
			return math.MinInt64, nil
		}
		return -int64(value), nil
	}

	return int64(value), nil
}

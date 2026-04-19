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
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("protocol: parse integer: %w", err)
		}
		return Integer{Value: value}, nil
	case '$':
		return p.parseBulkString()
	case '*':
		return p.parseArray()
	case '#':
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		switch line {
		case "t":
			return Boolean{Value: true}, nil
		case "f":
			return Boolean{Value: false}, nil
		default:
			return nil, fmt.Errorf("protocol: invalid boolean marker %q", line)
		}
	case '_':
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		if line != "" {
			return nil, fmt.Errorf("protocol: invalid null payload %q", line)
		}
		return Null{}, nil
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

	payload := make([]byte, length)
	if _, err := io.ReadFull(p.reader, payload); err != nil {
		return nil, fmt.Errorf("protocol: read bulk string payload: %w", err)
	}
	if err := p.expectCRLF(); err != nil {
		return nil, fmt.Errorf("protocol: bulk string payload missing CRLF terminator")
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

	elements := make([]Value, 0, count)
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
		return fmt.Errorf("protocol: line missing CRLF terminator")
	}

	return nil
}

func trimCRLF(line []byte) ([]byte, error) {
	if len(line) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return nil, fmt.Errorf("protocol: line missing CRLF terminator")
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
	if raw[0] == '-' {
		negative = true
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

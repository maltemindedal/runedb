package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Parser reads RESP values from an io.Reader.
type Parser struct {
	reader *bufio.Reader
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
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("protocol: parse bulk string length: %w", err)
	}
	if length == -1 {
		return BulkString{Null: true}, nil
	}
	if length < -1 {
		return nil, fmt.Errorf("protocol: invalid bulk string length %d", length)
	}

	buffer := make([]byte, length+2)
	if _, err := io.ReadFull(p.reader, buffer); err != nil {
		return nil, fmt.Errorf("protocol: read bulk string payload: %w", err)
	}
	if len(buffer) < 2 || buffer[len(buffer)-2] != '\r' || buffer[len(buffer)-1] != '\n' {
		return nil, fmt.Errorf("protocol: bulk string payload missing CRLF terminator")
	}

	payload := make([]byte, length)
	copy(payload, buffer[:length])
	return BulkString{Data: payload}, nil
}

func (p *Parser) parseArray() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	count, err := strconv.Atoi(line)
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
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("protocol: line missing CRLF terminator")
	}

	return strings.TrimSuffix(line, "\r\n"), nil
}

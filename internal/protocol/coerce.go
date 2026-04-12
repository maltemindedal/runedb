package protocol

import (
	"fmt"
	"strconv"
)

// Bytes coerces a RESP value into a byte slice suitable for command tokens.
func Bytes(value Value) ([]byte, error) {
	switch typed := value.(type) {
	case BulkString:
		if typed.Null {
			return nil, fmt.Errorf("protocol: null bulk string cannot be coerced to bytes")
		}
		return typed.Data, nil
	case TextBulkString:
		if typed.Null {
			return nil, fmt.Errorf("protocol: null bulk string cannot be coerced to bytes")
		}
		return []byte(typed.Value), nil
	case SimpleString:
		return []byte(typed.Value), nil
	case Integer:
		return []byte(strconv.FormatInt(typed.Value, 10)), nil
	case Boolean:
		if typed.Value {
			return []byte("1"), nil
		}
		return []byte("0"), nil
	case Null:
		return nil, fmt.Errorf("protocol: null cannot be coerced to bytes")
	default:
		return nil, fmt.Errorf("protocol: value type %T cannot be coerced to bytes", value)
	}
}

// String coerces a RESP value into a string suitable for command names or arguments.
func String(value Value) (string, error) {
	data, err := Bytes(value)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

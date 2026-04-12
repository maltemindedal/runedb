package protocol

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Encode serializes a RESP value into its wire representation.
func Encode(value Value) ([]byte, error) {
	var buffer bytes.Buffer
	if err := WriteValue(&buffer, value); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// EncodeValues serializes multiple RESP values into a single wire payload.
func EncodeValues(values []Value) ([]byte, error) {
	var buffer bytes.Buffer
	for _, value := range values {
		if err := WriteValue(&buffer, value); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

// WriteValue writes a RESP value to the provided writer.
func WriteValue(writer io.Writer, value Value) error {
	switch typed := value.(type) {
	case SimpleString:
		return writePrefixedStringLine(writer, '+', typed.Value)
	case ErrorValue:
		return writePrefixedStringLine(writer, '-', typed.Message)
	case Integer:
		return writePrefixedIntLine(writer, ':', typed.Value)
	case BulkString:
		if typed.Null {
			_, err := io.WriteString(writer, "$-1\r\n")
			return err
		}

		if err := writePrefixedIntLine(writer, '$', int64(len(typed.Data))); err != nil {
			return err
		}
		if _, err := writer.Write(typed.Data); err != nil {
			return err
		}
		_, err := io.WriteString(writer, "\r\n")
		return err
	case TextBulkString:
		if typed.Null {
			_, err := io.WriteString(writer, "$-1\r\n")
			return err
		}

		if err := writePrefixedIntLine(writer, '$', int64(len(typed.Value))); err != nil {
			return err
		}
		if _, err := io.WriteString(writer, typed.Value); err != nil {
			return err
		}
		_, err := io.WriteString(writer, "\r\n")
		return err
	case Array:
		if typed.Null {
			_, err := io.WriteString(writer, "*-1\r\n")
			return err
		}

		if err := writePrefixedIntLine(writer, '*', int64(len(typed.Elements))); err != nil {
			return err
		}
		for _, element := range typed.Elements {
			if err := WriteValue(writer, element); err != nil {
				return err
			}
		}
		return nil
	case Boolean:
		if typed.Value {
			_, err := io.WriteString(writer, "#t\r\n")
			return err
		}
		_, err := io.WriteString(writer, "#f\r\n")
		return err
	case Null:
		_, err := io.WriteString(writer, "_\r\n")
		return err
	default:
		return fmt.Errorf("protocol: unsupported value type %T", value)
	}
}

func writePrefixedStringLine(writer io.Writer, prefix byte, value string) error {
	if _, err := writer.Write([]byte{prefix}); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, value); err != nil {
		return err
	}
	_, err := io.WriteString(writer, "\r\n")
	return err
}

func writePrefixedIntLine(writer io.Writer, prefix byte, value int64) error {
	var buf [32]byte
	encoded := strconv.AppendInt(buf[:0], value, 10)
	if _, err := writer.Write([]byte{prefix}); err != nil {
		return err
	}
	if _, err := writer.Write(encoded); err != nil {
		return err
	}
	_, err := io.WriteString(writer, "\r\n")
	return err
}

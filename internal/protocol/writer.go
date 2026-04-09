package protocol

import (
	"bytes"
	"fmt"
	"io"
)

// Encode serializes a RESP value into its wire representation.
func Encode(value Value) ([]byte, error) {
	var buffer bytes.Buffer
	if err := WriteValue(&buffer, value); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// WriteValue writes a RESP value to the provided writer.
func WriteValue(writer io.Writer, value Value) error {
	switch typed := value.(type) {
	case SimpleString:
		_, err := fmt.Fprintf(writer, "+%s\r\n", typed.Value)
		return err
	case ErrorValue:
		_, err := fmt.Fprintf(writer, "-%s\r\n", typed.Message)
		return err
	case Integer:
		_, err := fmt.Fprintf(writer, ":%d\r\n", typed.Value)
		return err
	case BulkString:
		if typed.Null {
			_, err := io.WriteString(writer, "$-1\r\n")
			return err
		}

		if _, err := fmt.Fprintf(writer, "$%d\r\n", len(typed.Data)); err != nil {
			return err
		}
		if _, err := writer.Write(typed.Data); err != nil {
			return err
		}
		_, err := io.WriteString(writer, "\r\n")
		return err
	case Array:
		if typed.Null {
			_, err := io.WriteString(writer, "*-1\r\n")
			return err
		}

		if _, err := fmt.Fprintf(writer, "*%d\r\n", len(typed.Elements)); err != nil {
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

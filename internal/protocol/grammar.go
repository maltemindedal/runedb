package protocol

import "fmt"

// integerFromLine constructs a RESP integer value from a line body. A single
// optional leading sign is permitted, matching Redis integer frames.
func integerFromLine(line []byte) (Value, error) {
	value, err := parseIntBytes(line)
	if err != nil {
		return nil, fmt.Errorf("protocol: parse integer: %w", err)
	}
	return Integer{Value: value}, nil
}

// booleanFromMarker constructs a RESP3 boolean value from a marker line.
func booleanFromMarker(line []byte) (Value, error) {
	switch string(line) {
	case "t":
		return Boolean{Value: true}, nil
	case "f":
		return Boolean{Value: false}, nil
	default:
		return nil, fmt.Errorf("protocol: invalid boolean marker %q", string(line))
	}
}

// nullFromPayload constructs a RESP3 null value, rejecting any non-empty body.
func nullFromPayload(line []byte) (Value, error) {
	if len(line) != 0 {
		return nil, fmt.Errorf("protocol: invalid null payload %q", string(line))
	}
	return Null{}, nil
}

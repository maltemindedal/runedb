package protocol

// Value represents a RESP value exchanged on the wire.
type Value interface {
	value()
}

// SimpleString encodes a RESP simple string value.
type SimpleString struct {
	Value string
}

// ErrorValue encodes a RESP error value.
type ErrorValue struct {
	Message string
}

// Integer encodes a RESP integer value.
type Integer struct {
	Value int64
}

// BulkString encodes a RESP bulk string value.
type BulkString struct {
	Data []byte
	Null bool
}

// TextBulkString encodes a RESP bulk string backed by an immutable Go string.
type TextBulkString struct {
	Value string
	Null  bool
}

// Array encodes a RESP array value.
type Array struct {
	Elements []Value
	Null     bool
}

// Boolean is a RESP3 placeholder/value for future protocol expansion.
type Boolean struct {
	Value bool
}

// Null is a RESP3 placeholder/value for future protocol expansion.
type Null struct{}

func (SimpleString) value()   {}
func (ErrorValue) value()     {}
func (Integer) value()        {}
func (BulkString) value()     {}
func (TextBulkString) value() {}
func (Array) value()          {}
func (Boolean) value()        {}
func (Null) value()           {}

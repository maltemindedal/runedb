package command

import (
	"errors"
	"fmt"
)

var (
	// ErrSyntax reports malformed command syntax.
	ErrSyntax = errors.New("syntax error")
	// ErrInvalidExpireTime reports an invalid EX/PX duration.
	ErrInvalidExpireTime = errors.New("invalid expire time in 'SET' command")
	// ErrValueNotInteger reports that a value cannot be parsed as a 64-bit integer.
	ErrValueNotInteger = errors.New("value is not an integer or out of range")
	// ErrWrongType reports that a command targeted the wrong logical value type.
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
	// ErrNoAuth reports that the client must authenticate before running a command.
	ErrNoAuth = errors.New("authentication required")
)

// RESPError exposes the wire error prefix to use when returning a RESP error.
type RESPError interface {
	error
	RESPErrorPrefix() string
}

// ProtocolError represents malformed client protocol frames.
type ProtocolError struct {
	message string
}

func (e ProtocolError) Error() string {
	return e.message
}

// RESPErrorPrefix returns the Redis-compatible wire prefix for protocol errors.
func (e ProtocolError) RESPErrorPrefix() string {
	return "ERR"
}

type prefixedError struct {
	prefix  string
	message string
	cause   error
}

func (e *prefixedError) Error() string {
	return e.message
}

func (e *prefixedError) Unwrap() error {
	return e.cause
}

func (e *prefixedError) RESPErrorPrefix() string {
	return e.prefix
}

// ErrProtocol creates a new protocol validation error.
func ErrProtocol(message string) error {
	return ProtocolError{message: message}
}

// ErrUnknownCommand reports an unsupported command name.
func ErrUnknownCommand(name string) error {
	return newRESPMessageError("ERR", fmt.Sprintf("unknown command %q", name))
}

// ErrSyntaxError reports a Redis-style syntax error.
func ErrSyntaxError() error {
	return newRESPWrappedError("ERR", ErrSyntax)
}

// ErrInvalidExpireTimeError reports an invalid SET expiry argument.
func ErrInvalidExpireTimeError() error {
	return newRESPWrappedError("ERR", ErrInvalidExpireTime)
}

// ErrValueNotIntegerError reports that a string value cannot be incremented.
func ErrValueNotIntegerError() error {
	return newRESPWrappedError("ERR", ErrValueNotInteger)
}

// ErrWrongTypeError reports a Redis-style wrong-type failure.
func ErrWrongTypeError() error {
	return newRESPError("WRONGTYPE", "Operation against a key holding the wrong kind of value", ErrWrongType)
}

// ErrNoAuthError reports that the client must authenticate first.
func ErrNoAuthError() error {
	return newRESPError("NOAUTH", "Authentication required.", ErrNoAuth)
}

func wrongNumberOfArgumentsError(command string) error {
	return newRESPMessageError("ERR", fmt.Sprintf("wrong number of arguments for '%s' command", command))
}

func newRESPWrappedError(prefix string, cause error) error {
	return &prefixedError{prefix: prefix, message: cause.Error(), cause: cause}
}

func newRESPError(prefix, message string, cause error) error {
	return &prefixedError{prefix: prefix, message: message, cause: cause}
}

func newRESPMessageError(prefix, message string) error {
	return &prefixedError{prefix: prefix, message: message}
}

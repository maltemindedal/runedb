package aof

import (
	"errors"
	"fmt"
	"strings"
)

// Policy controls how often the AOF writer flushes buffered state to disk.
type Policy int

const (
	// PolicyAlways fsyncs before acknowledging the command.
	PolicyAlways Policy = iota
	// PolicyEverysec fsyncs once per second from a background goroutine.
	PolicyEverysec
	// PolicyNo leaves flushing to the operating system.
	PolicyNo
)

var (
	// ErrInvalidPolicy reports an unknown appendfsync policy.
	ErrInvalidPolicy = errors.New("aof: invalid appendfsync policy")
	// ErrRewriteInProgress reports that BGREWRITEAOF is already active.
	ErrRewriteInProgress = errors.New("append only file rewrite already in progress")
	// ErrClosed reports that the writer is no longer available.
	ErrClosed = errors.New("aof: writer closed")
)

// LoadStats summarizes AOF replay during startup.
type LoadStats struct {
	ReplayedCommands int
	TruncatedTail    bool
}

// RewriteStats summarizes BGREWRITEAOF payload generation.
type RewriteStats struct {
	Keys     int
	Commands int
}

// ParsePolicy parses the appendfsync policy name.
func ParsePolicy(raw string) (Policy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "everysec":
		return PolicyEverysec, nil
	case "always":
		return PolicyAlways, nil
	case "no":
		return PolicyNo, nil
	default:
		return PolicyEverysec, fmt.Errorf("%w %q", ErrInvalidPolicy, raw)
	}
}

// String returns the canonical flag value for the policy.
func (p Policy) String() string {
	switch p {
	case PolicyAlways:
		return "always"
	case PolicyNo:
		return "no"
	default:
		return "everysec"
	}
}

package command

import (
	"context"
	"strings"

	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/server"
)

func (e *Executor) handleSlowlog(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) == 0 {
		return nil, wrongNumberOfArgumentsError("SLOWLOG")
	}

	subcommand := strings.ToUpper(string(request.Args[0]))
	switch subcommand {
	case "GET":
		limit := -1
		if len(request.Args) == 2 {
			parsed, err := parseIntegerArgument(request.Args[1])
			if err != nil || parsed < 0 {
				return nil, ErrValueNotIntegerError()
			}
			limit = int(parsed)
		}
		return slowlogEntriesResponse(e.slowlogEntries(limit)), nil
	case "LEN":
		if e.slowlogRegistry == nil {
			return protocol.Integer{Value: 0}, nil
		}
		return protocol.Integer{Value: int64(e.slowlogRegistry.Len())}, nil
	case "RESET":
		if e.slowlogRegistry != nil {
			e.slowlogRegistry.Reset()
		}
		return protocol.SimpleString{Value: "OK"}, nil
	default:
		return nil, ErrSyntaxError()
	}
}

func validateSlowlogRequest(request *Request) error {
	if len(request.Args) == 0 {
		return wrongNumberOfArgumentsError("SLOWLOG")
	}

	subcommand := strings.ToUpper(string(request.Args[0]))
	switch subcommand {
	case "GET":
		if len(request.Args) > 2 {
			return wrongNumberOfArgumentsError("SLOWLOG")
		}
		if len(request.Args) == 2 {
			parsed, err := parseIntegerArgument(request.Args[1])
			if err != nil || parsed < 0 {
				return ErrValueNotIntegerError()
			}
		}
	case "LEN", "RESET":
		if len(request.Args) != 1 {
			return wrongNumberOfArgumentsError("SLOWLOG")
		}
	default:
		return ErrSyntaxError()
	}
	return nil
}

func (e *Executor) slowlogEntries(limit int) []server.SlowlogEntry {
	if e.slowlogRegistry == nil {
		return nil
	}
	return e.slowlogRegistry.Entries(limit)
}

func slowlogEntriesResponse(entries []server.SlowlogEntry) protocol.Array {
	elements := make([]protocol.Value, 0, len(entries))
	for _, entry := range entries {
		argv := make([]protocol.Value, 0, len(entry.Command))
		for _, token := range entry.Command {
			argv = append(argv, protocol.TextBulkString{Value: token})
		}

		elements = append(elements, protocol.Array{Elements: []protocol.Value{
			protocol.Integer{Value: entry.ID},
			protocol.Integer{Value: entry.Timestamp.Unix()},
			protocol.Integer{Value: entry.Duration.Microseconds()},
			protocol.Array{Elements: argv},
			protocol.TextBulkString{Value: entry.ClientAddr},
			protocol.TextBulkString{Value: ""},
		}})
	}

	return protocol.Array{Elements: elements}
}

func requestTokens(request *Request) []string {
	if request == nil {
		return nil
	}

	tokens := make([]string, 0, len(request.Args)+1)
	tokens = append(tokens, request.Name)
	if isSensitiveSlowlogCommand(request.Name) {
		for range request.Args {
			tokens = append(tokens, "[redacted]")
		}
		return tokens
	}

	for _, arg := range request.Args {
		tokens = append(tokens, string(arg))
	}
	return tokens
}

func isSensitiveSlowlogCommand(name string) bool {
	switch name {
	case "AUTH":
		return true
	default:
		return false
	}
}

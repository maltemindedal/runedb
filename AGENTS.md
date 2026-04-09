# AGENTS.md

## Quick Commands
- `go test ./...` runs the full test suite.
- `go test -race ./...` is the concurrency-focused verification pass already documented by the repo.
- `golangci-lint run` runs the configured linters from `.golangci.yml` (`errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`).
- Run one package with `go test ./internal/protocol` or similar.
- Run one test with `go test ./test -run TestServerHandlesPhaseOneCommands` or `go test ./internal/command -run TestExecutorExecute`.
- Start the server with `go run ./cmd/godis --port 6379`.

## Required Workflow
- Before any agentic implementation or investigation, check `.agents/skills/` for relevant skills and load/use the matching guidance.
- Before making technical decisions, check the relevant library or tool docs via the Context7 MCP server; prefer those docs over memory when they conflict.

## Architecture
- The only production entrypoint is `cmd/godis/main.go`: it wires `config.ParseFlags()` -> logger -> `storage.NewStore()` -> `command.NewExecutor()` -> `server.New()`.
- Request flow is `internal/server` parse loop -> `command.DecodeRequest` / `Executor.Execute` -> RESP write-back. If behavior changes at the protocol boundary, inspect both `internal/server/handler.go` and `internal/command`.
- Current command surface is only `PING`, `ECHO`, `SET`, and `GET`. `Executor` registers handlers in `internal/command/types.go`.
- `internal/storage` is intentionally simple: one global `sync.RWMutex`, string values only, TTL stored as Unix milliseconds.
- TTL eviction happens in two places: passive eviction on `Get`, and active background sampling via `Store.StartEviction()`.

## Protocol / Behavior Quirks
- The server is Phase 1 / RESP2-centric even though `internal/protocol` already contains RESP3 placeholder types like `Boolean` and `Null`. Do not assume broader RESP3 server support exists.
- Command names are normalized to uppercase in `DecodeRequest`, so handler registration and error expectations are case-insensitive at the wire level.
- `PING` supports an optional single payload and returns it as a bulk string, matching the current tests.
- `SET` only supports optional `EX` / `PX`; unsupported modifiers should still fail with syntax-style errors, not be silently ignored.

## Testing Notes
- `test/integration_test.go` boots a real TCP server on `127.0.0.1` with port `0`; integration coverage does not require external services.
- TTL tests rely on short sleeps and short eviction intervals. Keep timing margins generous if you change expiration behavior or background eviction cadence.
- Server shutdown is context-driven: `server.NotifyContext()` handles signals in production, while tests cancel contexts directly.

## Constraints That Matter
- There is no Makefile, Taskfile, CI workflow, or existing repo-local agent instruction set; use raw Go commands instead of guessing wrapper scripts.
- `Config.RequirePass` is a placeholder flag only; AUTH is not implemented.

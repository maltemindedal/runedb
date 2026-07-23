package command

import (
	"context"
	"fmt"

	"github.com/maltemindedal/stash/internal/protocol"
)

func (e *Executor) handleMonitor(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 0 {
		return nil, wrongNumberOfArgumentsError("MONITOR")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if state.InTransactionActive() {
		return nil, fmt.Errorf("MONITOR inside MULTI is not allowed")
	}
	if state.IsSubscribed() {
		return nil, ErrSubscribedModeOnlyError()
	}
	if e.monitorRegistry == nil {
		return nil, fmt.Errorf("monitor registry unavailable")
	}

	state.StartMonitoring()
	return protocol.SimpleString{Value: "OK"}, nil
}

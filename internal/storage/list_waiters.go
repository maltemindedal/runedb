package storage

import "sync"

type listWaiters struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func newListWaiters() *listWaiters {
	return &listWaiters{waiters: make(map[string][]chan struct{})}
}

func (w *listWaiters) subscribe(key string) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	ch := make(chan struct{}, 1)
	w.waiters[key] = append(w.waiters[key], ch)
	return ch
}

func (w *listWaiters) unsubscribe(key string, ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	waiters := w.waiters[key]
	for i, waiter := range waiters {
		if waiter != ch {
			continue
		}

		waiters = append(waiters[:i], waiters[i+1:]...)
		if len(waiters) == 0 {
			delete(w.waiters, key)
		} else {
			w.waiters[key] = waiters
		}
		return
	}
}

func (w *listWaiters) notifyOne(key string) {
	w.mu.Lock()
	waiters := w.waiters[key]
	if len(waiters) == 0 {
		w.mu.Unlock()
		return
	}

	ch := waiters[0]
	remaining := append(waiters[:0:0], waiters[1:]...)
	if len(remaining) == 0 {
		delete(w.waiters, key)
	} else {
		w.waiters[key] = remaining
	}
	w.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}
}

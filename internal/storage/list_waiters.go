package storage

import (
	"container/list"
	"sync"
)

type listWaiters struct {
	mu      sync.Mutex
	waiters map[string]*waiterQueue
}

type waiterQueue struct {
	order *list.List
	index map[chan struct{}]*list.Element
}

func newListWaiters() *listWaiters {
	return &listWaiters{waiters: make(map[string]*waiterQueue)}
}

func (w *listWaiters) subscribe(key string) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	queue := w.waiters[key]
	if queue == nil {
		queue = &waiterQueue{order: list.New(), index: make(map[chan struct{}]*list.Element)}
		w.waiters[key] = queue
	}

	ch := make(chan struct{}, 1)
	queue.index[ch] = queue.order.PushBack(ch)
	return ch
}

func (w *listWaiters) unsubscribe(key string, ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	queue := w.waiters[key]
	if queue == nil {
		return
	}

	element, ok := queue.index[ch]
	if !ok {
		return
	}

	queue.order.Remove(element)
	delete(queue.index, ch)
	if queue.order.Len() == 0 {
		delete(w.waiters, key)
	}
}

func (w *listWaiters) notifyOne(key string) {
	w.mu.Lock()
	queue := w.waiters[key]
	if queue == nil || queue.order.Len() == 0 {
		w.mu.Unlock()
		return
	}

	front := queue.order.Front()
	ch := front.Value.(chan struct{})
	queue.order.Remove(front)
	delete(queue.index, ch)
	if queue.order.Len() == 0 {
		delete(w.waiters, key)
	}
	w.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}
}

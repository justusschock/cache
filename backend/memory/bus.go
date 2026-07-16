package memory

import (
	"context"
	"errors"
	"sync"

	cache "github.com/justusschock/go-cache"
)

// InvalidationBus distributes events between workers in the same process.
type InvalidationBus[K comparable] struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[string]map[uint64]cache.InvalidationHandler[K]
}

// NewInvalidationBus constructs an in-process invalidation bus.
func NewInvalidationBus[K comparable]() *InvalidationBus[K] {
	return &InvalidationBus[K]{
		subscribers: make(map[string]map[uint64]cache.InvalidationHandler[K]),
	}
}

// Publish delivers an event to the current subscribers of its namespace.
func (b *InvalidationBus[K]) Publish(ctx context.Context, event cache.InvalidationEvent[K]) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	handlers := make([]cache.InvalidationHandler[K], 0, len(b.subscribers[event.Namespace]))
	for _, handler := range b.subscribers[event.Namespace] {
		handlers = append(handlers, handler)
	}
	b.mu.Unlock()

	var joined error
	for _, handler := range handlers {
		if err := handler(ctx, event); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

// Subscribe registers a handler until the context is canceled.
func (b *InvalidationBus[K]) Subscribe(ctx context.Context, namespace string, handler cache.InvalidationHandler[K]) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	b.nextID++
	id := b.nextID
	if b.subscribers[namespace] == nil {
		b.subscribers[namespace] = make(map[uint64]cache.InvalidationHandler[K])
	}
	b.subscribers[namespace][id] = handler
	b.mu.Unlock()

	<-ctx.Done()

	b.mu.Lock()
	delete(b.subscribers[namespace], id)
	if len(b.subscribers[namespace]) == 0 {
		delete(b.subscribers, namespace)
	}
	b.mu.Unlock()
	return ctx.Err()
}

// SubscriberCount returns the number of active namespace subscribers.
func (b *InvalidationBus[K]) SubscriberCount(namespace string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers[namespace])
}

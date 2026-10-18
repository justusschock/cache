package cache

import (
	"context"
	"errors"
)

// ErrNilInvalidationHandler reports a nil handler passed to
// InvalidationBus.Subscribe.
var ErrNilInvalidationHandler = errors.New("cache: invalidation handler must not be nil")

// InvalidationKind identifies the scope of an invalidation event.
type InvalidationKind uint8

const (
	// InvalidateKey invalidates one key in a namespace.
	InvalidateKey InvalidationKind = iota + 1
	// InvalidateNamespace invalidates every key in a namespace.
	InvalidateNamespace
)

// InvalidationEvent is sent between cache workers after a mutation.
type InvalidationEvent[K comparable] struct {
	Namespace  string
	Kind       InvalidationKind
	Key        K
	Generation uint64
}

// InvalidationHandler applies a remotely published invalidation event.
type InvalidationHandler[K comparable] func(context.Context, InvalidationEvent[K]) error

// InvalidationBus publishes and subscribes to cache invalidation events.
type InvalidationBus[K comparable] interface {
	Publish(context.Context, InvalidationEvent[K]) error
	Subscribe(context.Context, string, InvalidationHandler[K]) error
}

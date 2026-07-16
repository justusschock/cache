package cache

import (
	"context"
	"time"
)

// Entry is the value and logical freshness metadata exchanged with stores.
// Found is false for an authoritative negative-cache entry.
type Entry[V any] struct {
	Value     V
	Found     bool
	ExpiresAt time.Time
}

// Store persists typed entries for one or more namespaces.
type Store[K comparable, V any] interface {
	Get(context.Context, string, K) (Entry[V], bool, error)
	Set(context.Context, string, K, Entry[V]) error
	Invalidate(context.Context, string, K) error
	InvalidateAll(context.Context, string) error
}

package cache

import (
	"context"
	"errors"
	"time"
)

// ErrBatchLength reports batch arguments whose key and value slices differ in length.
var ErrBatchLength = errors.New("cache: batch keys and values must have equal length")

// Entry is the value and logical freshness metadata exchanged with stores.
// Found is false for an authoritative negative-cache entry. A zero ExpiresAt
// marks an entry that never expires; stores must neither hide it as expired
// nor attach a backend-native TTL to it.
type Entry[V any] struct {
	Value     V
	Found     bool
	ExpiresAt time.Time
}

// NeverExpires reports whether the entry has no expiration (zero ExpiresAt).
func (e Entry[V]) NeverExpires() bool {
	return e.ExpiresAt.IsZero()
}

// Expired reports whether the entry is logically expired at now. Entries
// without an expiration are never expired.
func (e Entry[V]) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(now)
}

// Store persists typed entries for one or more namespaces.
type Store[K comparable, V any] interface {
	Get(context.Context, string, K) (Entry[V], bool, error)
	Set(context.Context, string, K, Entry[V]) error
	Invalidate(context.Context, string, K) error
	InvalidateAll(context.Context, string) error
}

// BatchStore is an optional Store extension for multi-key operations.
//
// GetMany returns entries and presence flags positionally aligned with keys,
// applying the same visibility rules as Get. SetMany writes entries[i] under
// keys[i], returns ErrBatchLength when the slices differ in length, and lets
// a later duplicate key win. InvalidateMany removes every listed key and is
// idempotent for missing keys. Atomicity is backend-specific: unless an
// implementation documents otherwise, a failed call may have applied part of
// the batch.
type BatchStore[K comparable, V any] interface {
	Store[K, V]
	GetMany(context.Context, string, []K) ([]Entry[V], []bool, error)
	SetMany(context.Context, string, []K, []Entry[V]) error
	InvalidateMany(context.Context, string, []K) error
}

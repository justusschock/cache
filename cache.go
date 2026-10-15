package cache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	ErrEmptyNamespace = errors.New("cache: namespace must not be empty")
	ErrInvalidTTL     = errors.New("cache: ttl must be positive")
	ErrNilStore       = errors.New("cache: store must not be nil")
)

// NoExpiry is the explicit opt-in TTL for entries that never expire. New and
// SetWithTTL accept it in place of a positive duration; the resulting entries
// carry a zero ExpiresAt and remain until they are invalidated or evicted by
// the store. Every other non-positive TTL is still rejected with ErrInvalidTTL.
const NoExpiry time.Duration = math.MinInt64

// Cache is the typed cache API exposed to callers.
type Cache[K comparable, V any] interface {
	Get(context.Context, K) (V, bool, error)
	Set(context.Context, K, V) error
	SetWithTTL(context.Context, K, V, time.Duration) error
	Invalidate(context.Context, K) error
	InvalidateAll(context.Context) error
}

type EntryCache[K comparable, V any] interface {
	Cache[K, V]
	GetEntry(context.Context, K) (Entry[V], bool, error)
	SetEntry(context.Context, K, Entry[V]) error
}

// BatchCache is an optional Cache extension for multi-key operations. Results
// are positionally aligned with keys, and value slices must match the key
// slice length.
type BatchCache[K comparable, V any] interface {
	Cache[K, V]
	GetMany(context.Context, []K) ([]V, []bool, error)
	SetMany(context.Context, []K, []V) error
	SetManyWithTTL(context.Context, []K, []V, time.Duration) error
	InvalidateMany(context.Context, []K) error
}

// StandardCache applies namespace and TTL policy to a Store.
type StandardCache[K comparable, V any] struct {
	namespace  string
	defaultTTL time.Duration
	store      Store[K, V]
}

// New constructs a cache with a required namespace and positive default TTL.
// Pass NoExpiry as defaultTTL to make Set and SetMany write entries that never expire.
func New[K comparable, V any](namespace string, defaultTTL time.Duration, store Store[K, V]) (*StandardCache[K, V], error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}
	if !validTTL(defaultTTL) {
		return nil, ErrInvalidTTL
	}
	if isNil(store) {
		return nil, ErrNilStore
	}
	return &StandardCache[K, V]{namespace: namespace, defaultTTL: defaultTTL, store: store}, nil
}

func (c *StandardCache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	entry, present, err := c.GetEntry(ctx, key)
	if err != nil {
		return zero, false, err
	}
	if !present || !entry.Found {
		return zero, false, nil
	}
	return entry.Value, true, nil
}

func (c *StandardCache[K, V]) Set(ctx context.Context, key K, value V) error {
	return c.SetWithTTL(ctx, key, value, c.defaultTTL)
}

// SetWithTTL stores value for ttl, or without expiration when ttl is NoExpiry.
func (c *StandardCache[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	if !validTTL(ttl) {
		return ErrInvalidTTL
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	entry := Entry[V]{Value: value, Found: true, ExpiresAt: expiresAt(ttl)}
	if err := c.store.Set(ctx, c.namespace, key, entry); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

func (c *StandardCache[K, V]) Invalidate(ctx context.Context, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.store.Invalidate(ctx, c.namespace, key); err != nil {
		return fmt.Errorf("cache invalidate: %w", err)
	}
	return nil
}

func (c *StandardCache[K, V]) InvalidateAll(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.store.InvalidateAll(ctx, c.namespace); err != nil {
		return fmt.Errorf("cache invalidate all: %w", err)
	}
	return nil
}

func (c *StandardCache[K, V]) GetEntry(ctx context.Context, key K) (Entry[V], bool, error) {
	var zero Entry[V]
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	entry, present, err := c.store.Get(ctx, c.namespace, key)
	if err != nil {
		return zero, false, fmt.Errorf("cache get: %w", err)
	}
	if !present || entry.Expired(time.Now()) {
		return zero, false, nil
	}
	return entry, true, nil
}

// SetEntry stores entry as-is. It rejects entries that are already expired;
// a zero ExpiresAt stores an entry that never expires.
func (c *StandardCache[K, V]) SetEntry(ctx context.Context, key K, entry Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.Expired(time.Now()) {
		return ErrInvalidTTL
	}
	if err := c.store.Set(ctx, c.namespace, key, entry); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

// GetMany looks up keys and returns values and found flags aligned with keys.
// Missing, expired, and negative entries report found as false. It uses a
// single BatchStore call when the store supports batching and otherwise falls
// back to one Get per key.
func (c *StandardCache[K, V]) GetMany(ctx context.Context, keys []K) ([]V, []bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	values := make([]V, len(keys))
	found := make([]bool, len(keys))
	if len(keys) == 0 {
		return values, found, nil
	}
	entries, present, err := c.getMany(ctx, keys)
	if err != nil {
		return nil, nil, fmt.Errorf("cache get many: %w", err)
	}
	now := time.Now()
	for index := range keys {
		if present[index] && entries[index].Found && !entries[index].Expired(now) {
			values[index] = entries[index].Value
			found[index] = true
		}
	}
	return values, found, nil
}

// SetMany stores values[i] under keys[i] with the default TTL.
func (c *StandardCache[K, V]) SetMany(ctx context.Context, keys []K, values []V) error {
	return c.SetManyWithTTL(ctx, keys, values, c.defaultTTL)
}

// SetManyWithTTL stores values[i] under keys[i] for ttl, or without expiration
// when ttl is NoExpiry. It uses a single BatchStore call when the store
// supports batching and otherwise falls back to one Set per key, stopping at
// the first error.
func (c *StandardCache[K, V]) SetManyWithTTL(ctx context.Context, keys []K, values []V, ttl time.Duration) error {
	if !validTTL(ttl) {
		return ErrInvalidTTL
	}
	if len(keys) != len(values) {
		return ErrBatchLength
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	expiration := expiresAt(ttl)
	entries := make([]Entry[V], len(values))
	for index, value := range values {
		entries[index] = Entry[V]{Value: value, Found: true, ExpiresAt: expiration}
	}
	if batch, ok := c.store.(BatchStore[K, V]); ok {
		if err := batch.SetMany(ctx, c.namespace, keys, entries); err != nil {
			return fmt.Errorf("cache set many: %w", err)
		}
		return nil
	}
	for index, key := range keys {
		if err := c.store.Set(ctx, c.namespace, key, entries[index]); err != nil {
			return fmt.Errorf("cache set many: %w", err)
		}
	}
	return nil
}

// InvalidateMany removes keys. It uses a single BatchStore call when the store
// supports batching and otherwise falls back to one Invalidate per key,
// stopping at the first error.
func (c *StandardCache[K, V]) InvalidateMany(ctx context.Context, keys []K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if batch, ok := c.store.(BatchStore[K, V]); ok {
		if err := batch.InvalidateMany(ctx, c.namespace, keys); err != nil {
			return fmt.Errorf("cache invalidate many: %w", err)
		}
		return nil
	}
	for _, key := range keys {
		if err := c.store.Invalidate(ctx, c.namespace, key); err != nil {
			return fmt.Errorf("cache invalidate many: %w", err)
		}
	}
	return nil
}

func (c *StandardCache[K, V]) getMany(ctx context.Context, keys []K) ([]Entry[V], []bool, error) {
	if batch, ok := c.store.(BatchStore[K, V]); ok {
		entries, present, err := batch.GetMany(ctx, c.namespace, keys)
		if err != nil {
			return nil, nil, err
		}
		if len(entries) != len(keys) || len(present) != len(keys) {
			return nil, nil, ErrBatchLength
		}
		return entries, present, nil
	}
	entries := make([]Entry[V], len(keys))
	present := make([]bool, len(keys))
	for index, key := range keys {
		entry, ok, err := c.store.Get(ctx, c.namespace, key)
		if err != nil {
			return nil, nil, err
		}
		entries[index] = entry
		present[index] = ok
	}
	return entries, present, nil
}

func validTTL(ttl time.Duration) bool {
	return ttl > 0 || ttl == NoExpiry
}

func expiresAt(ttl time.Duration) time.Time {
	if ttl == NoExpiry {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

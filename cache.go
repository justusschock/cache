package cache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrEmptyNamespace = errors.New("cache: namespace must not be empty")
	ErrInvalidTTL     = errors.New("cache: ttl must be positive")
	ErrNilStore       = errors.New("cache: store must not be nil")
)

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

// StandardCache applies namespace and TTL policy to a Store.
type StandardCache[K comparable, V any] struct {
	namespace  string
	defaultTTL time.Duration
	store      Store[K, V]
}

// New constructs a cache with a required namespace and positive default TTL.
func New[K comparable, V any](namespace string, defaultTTL time.Duration, store Store[K, V]) (*StandardCache[K, V], error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}
	if defaultTTL <= 0 {
		return nil, ErrInvalidTTL
	}
	if store == nil {
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

func (c *StandardCache[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	entry := Entry[V]{Value: value, Found: true, ExpiresAt: time.Now().Add(ttl)}
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
	if !present || !entry.ExpiresAt.After(time.Now()) {
		return zero, false, nil
	}
	return entry, true, nil
}

func (c *StandardCache[K, V]) SetEntry(ctx context.Context, key K, entry Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !entry.ExpiresAt.After(time.Now()) {
		return ErrInvalidTTL
	}
	if err := c.store.Set(ctx, c.namespace, key, entry); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

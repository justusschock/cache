package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrNilCache    = errors.New("cache: wrapped cache must not be nil")
	ErrNilLoader   = errors.New("cache: loader must not be nil")
	ErrNilLocker   = errors.New("cache: locker must not be nil")
	ErrLoaderPanic = errors.New("cache: loader panicked")
)

// Loader returns a value, an authoritative found state, or an error.
type Loader[K comparable, V any] func(context.Context, K) (V, bool, error)

type pullThroughConfig[K comparable] struct {
	negativeTTL    time.Duration
	locker         Locker[K]
	leaseTTL       time.Duration
	pollInterval   time.Duration
	loadOnLockErr  bool
	strictCacheGet bool
}

// PullThroughOption configures pull-through behavior.
type PullThroughOption[K comparable] func(*pullThroughConfig[K]) error

// WithNegativeTTL sets the lifetime of authoritative not-found entries.
func WithNegativeTTL[K comparable](ttl time.Duration) PullThroughOption[K] {
	return func(config *pullThroughConfig[K]) error {
		if ttl <= 0 {
			return ErrInvalidTTL
		}
		config.negativeTTL = ttl
		return nil
	}
}

// WithLocker enables distributed miss coordination using renewable leases.
func WithLocker[K comparable](locker Locker[K], leaseTTL time.Duration) PullThroughOption[K] {
	return func(config *pullThroughConfig[K]) error {
		if locker == nil {
			return ErrNilLocker
		}
		if leaseTTL <= 0 {
			return ErrInvalidTTL
		}
		config.locker = locker
		config.leaseTTL = leaseTTL
		return nil
	}
}

// WithLockPollInterval controls how often a contended lock is retried.
func WithLockPollInterval[K comparable](interval time.Duration) PullThroughOption[K] {
	return func(config *pullThroughConfig[K]) error {
		if interval <= 0 {
			return ErrInvalidTTL
		}
		config.pollInterval = interval
		return nil
	}
}

// WithLoadOnLockError permits loading when the distributed locker fails.
func WithLoadOnLockError[K comparable]() PullThroughOption[K] {
	return func(config *pullThroughConfig[K]) error {
		config.loadOnLockErr = true
		return nil
	}
}

// WithStrictPullThroughReads returns cache read errors instead of loading.
func WithStrictPullThroughReads[K comparable]() PullThroughOption[K] {
	return func(config *pullThroughConfig[K]) error {
		config.strictCacheGet = true
		return nil
	}
}

type entryAccess[K comparable, V any] interface {
	getEntry(context.Context, K) (Entry[V], bool, error)
	setEntry(context.Context, K, Entry[V]) error
}

type lookupState uint8

const (
	lookupMissing lookupState = iota
	lookupFound
	lookupNegative
)

// PullThroughCache loads and stores values on cache misses.
type PullThroughCache[K comparable, V any] struct {
	namespace string
	cache     Cache[K, V]
	loader    Loader[K, V]
	config    pullThroughConfig[K]
	flights   flightGroup[K, V]

	negativeMu sync.Mutex
	negative   map[K]time.Time
}

// NewPullThrough wraps a cache with automatic miss loading.
func NewPullThrough[K comparable, V any](namespace string, wrapped Cache[K, V], loader Loader[K, V], options ...PullThroughOption[K]) (*PullThroughCache[K, V], error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}
	if wrapped == nil {
		return nil, ErrNilCache
	}
	if loader == nil {
		return nil, ErrNilLoader
	}
	config := pullThroughConfig[K]{negativeTTL: 30 * time.Second, pollInterval: 25 * time.Millisecond}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &PullThroughCache[K, V]{
		namespace: namespace,
		cache:     wrapped,
		loader:    loader,
		config:    config,
		negative:  make(map[K]time.Time),
	}, nil
}

func (c *PullThroughCache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	value, state, err := c.lookup(ctx, key)
	if err != nil {
		var zero V
		return zero, false, err
	}
	if state == lookupFound {
		return value, true, nil
	}
	if state == lookupNegative {
		var zero V
		return zero, false, nil
	}

	return c.flights.Do(ctx, key, func(loadCtx context.Context) (V, bool, error) {
		value, state, err := c.lookup(loadCtx, key)
		if err != nil {
			var zero V
			return zero, false, err
		}
		if state == lookupFound {
			return value, true, nil
		}
		if state == lookupNegative {
			var zero V
			return zero, false, nil
		}
		return c.load(loadCtx, key)
	})
}

func (c *PullThroughCache[K, V]) Set(ctx context.Context, key K, value V) error {
	c.clearLocalNegative(key)
	return c.cache.Set(ctx, key, value)
}

func (c *PullThroughCache[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	c.clearLocalNegative(key)
	return c.cache.SetWithTTL(ctx, key, value, ttl)
}

func (c *PullThroughCache[K, V]) Invalidate(ctx context.Context, key K) error {
	c.clearLocalNegative(key)
	return c.cache.Invalidate(ctx, key)
}

func (c *PullThroughCache[K, V]) InvalidateAll(ctx context.Context) error {
	c.negativeMu.Lock()
	clear(c.negative)
	c.negativeMu.Unlock()
	return c.cache.InvalidateAll(ctx)
}

func (c *PullThroughCache[K, V]) lookup(ctx context.Context, key K) (V, lookupState, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, lookupMissing, err
	}
	if c.isLocalNegative(key) {
		return zero, lookupNegative, nil
	}
	if access, ok := c.cache.(entryAccess[K, V]); ok {
		entry, present, err := access.getEntry(ctx, key)
		if err != nil {
			if c.config.strictCacheGet {
				return zero, lookupMissing, err
			}
			return zero, lookupMissing, nil
		}
		if !present {
			return zero, lookupMissing, nil
		}
		if !entry.Found {
			return zero, lookupNegative, nil
		}
		return entry.Value, lookupFound, nil
	}
	value, found, err := c.cache.Get(ctx, key)
	if err != nil {
		if c.config.strictCacheGet {
			return zero, lookupMissing, err
		}
		return zero, lookupMissing, nil
	}
	if !found {
		return zero, lookupMissing, nil
	}
	return value, lookupFound, nil
}

func (c *PullThroughCache[K, V]) load(ctx context.Context, key K) (V, bool, error) {
	var lease Lease
	if c.config.locker != nil {
		acquired, resolvedValue, resolvedFound, resolved, err := c.acquire(ctx, key)
		if err != nil {
			var zero V
			return zero, false, err
		}
		if resolved {
			return resolvedValue, resolvedFound, nil
		}
		lease = acquired
		if lease != nil {
			value, state, lookupErr := c.lookup(ctx, key)
			if lookupErr != nil {
				var zero V
				return zero, false, lookupErr
			}
			if state == lookupFound {
				_ = c.release(ctx, lease)
				return value, true, nil
			}
			if state == lookupNegative {
				_ = c.release(ctx, lease)
				var zero V
				return zero, false, nil
			}
		}
	}

	stopRenewal := func() {}
	if lease != nil {
		stopRenewal = c.startRenewal(ctx, lease)
	}
	value, found, loadErr := c.callLoader(ctx, key)
	stopRenewal()
	if lease != nil {
		loadErr = errors.Join(loadErr, c.release(ctx, lease))
	}
	if loadErr != nil {
		var zero V
		return zero, false, loadErr
	}
	if found {
		if err := c.cache.Set(ctx, key, value); err != nil {
			return value, true, err
		}
		return value, true, nil
	}
	if err := c.storeNegative(ctx, key); err != nil {
		return value, false, err
	}
	return value, false, nil
}

func (c *PullThroughCache[K, V]) acquire(ctx context.Context, key K) (Lease, V, bool, bool, error) {
	var zero V
	for {
		lease, err := c.config.locker.Lock(ctx, c.namespace, key, c.config.leaseTTL)
		if err == nil {
			return lease, zero, false, false, nil
		}
		if !errors.Is(err, ErrLockNotAcquired) {
			if c.config.loadOnLockErr {
				return nil, zero, false, false, nil
			}
			return nil, zero, false, false, err
		}

		value, state, lookupErr := c.lookup(ctx, key)
		if lookupErr != nil {
			return nil, zero, false, false, lookupErr
		}
		if state == lookupFound {
			return nil, value, true, true, nil
		}
		if state == lookupNegative {
			return nil, zero, false, true, nil
		}

		timer := time.NewTimer(c.config.pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, zero, false, false, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *PullThroughCache[K, V]) callLoader(ctx context.Context, key K) (value V, found bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero V
			value = zero
			found = false
			err = fmt.Errorf("%w: %v", ErrLoaderPanic, recovered)
		}
	}()
	return c.loader(ctx, key)
}

func (c *PullThroughCache[K, V]) storeNegative(ctx context.Context, key K) error {
	expiresAt := time.Now().Add(c.config.negativeTTL)
	if access, ok := c.cache.(entryAccess[K, V]); ok {
		var zero V
		return access.setEntry(ctx, key, Entry[V]{Value: zero, Found: false, ExpiresAt: expiresAt})
	}
	c.negativeMu.Lock()
	c.negative[key] = expiresAt
	c.negativeMu.Unlock()
	return nil
}

func (c *PullThroughCache[K, V]) isLocalNegative(key K) bool {
	c.negativeMu.Lock()
	defer c.negativeMu.Unlock()
	expiresAt, ok := c.negative[key]
	if !ok {
		return false
	}
	if !expiresAt.After(time.Now()) {
		delete(c.negative, key)
		return false
	}
	return true
}

func (c *PullThroughCache[K, V]) clearLocalNegative(key K) {
	c.negativeMu.Lock()
	delete(c.negative, key)
	c.negativeMu.Unlock()
}

func (c *PullThroughCache[K, V]) startRenewal(ctx context.Context, lease Lease) func() {
	renewalCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			_ = recover()
		}()
		ticker := time.NewTicker(c.config.leaseTTL / 2)
		defer ticker.Stop()
		for {
			select {
			case <-renewalCtx.Done():
				return
			case <-ticker.C:
				if err := lease.Refresh(renewalCtx, c.config.leaseTTL); err != nil {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (c *PullThroughCache[K, V]) release(ctx context.Context, lease Lease) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.leaseTTL)
	defer cancel()
	return lease.Release(releaseCtx)
}

package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors returned by NewPullThrough and PullThroughCache.Get.
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
		if isNil(locker) {
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
}

// NewPullThrough wraps a cache with automatic miss loading.
func NewPullThrough[K comparable, V any](namespace string, wrapped Cache[K, V], loader Loader[K, V], options ...PullThroughOption[K]) (*PullThroughCache[K, V], error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}
	if isNil(wrapped) {
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
	}, nil
}

// Get returns the cached value for key, loading it on a miss. Concurrent
// misses for one key share a single load; each caller waits with its own
// ctx, and the load is canceled once every waiter has left. Authoritative
// misses are cached for the negative TTL when the wrapped cache is an
// EntryCache; loader errors are never cached.
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

// Set delegates to the wrapped cache.
func (c *PullThroughCache[K, V]) Set(ctx context.Context, key K, value V) error {
	return c.cache.Set(ctx, key, value)
}

// SetWithTTL delegates to the wrapped cache.
func (c *PullThroughCache[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	return c.cache.SetWithTTL(ctx, key, value, ttl)
}

// Invalidate delegates to the wrapped cache.
func (c *PullThroughCache[K, V]) Invalidate(ctx context.Context, key K) error {
	return c.cache.Invalidate(ctx, key)
}

// InvalidateAll delegates to the wrapped cache.
func (c *PullThroughCache[K, V]) InvalidateAll(ctx context.Context) error {
	return c.cache.InvalidateAll(ctx)
}

func (c *PullThroughCache[K, V]) lookup(ctx context.Context, key K) (V, lookupState, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, lookupMissing, err
	}
	if access, ok := c.cache.(EntryCache[K, V]); ok {
		entry, present, err := access.GetEntry(ctx, key)
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
			releaseErr := error(nil)
			if lookupErr != nil {
				releaseErr = c.release(ctx, lease)
				var zero V
				return zero, false, errors.Join(lookupErr, releaseErr)
			}
			if state == lookupFound {
				releaseErr = c.release(ctx, lease)
				return value, true, releaseErr
			}
			if state == lookupNegative {
				releaseErr = c.release(ctx, lease)
				var zero V
				return zero, false, releaseErr
			}
		}
	}

	if lease == nil {
		return c.loadAndStore(ctx, key)
	}
	return c.loadAndStoreWithLease(ctx, key, lease)
}

func (c *PullThroughCache[K, V]) acquire(ctx context.Context, key K) (Lease, V, bool, bool, error) {
	var zero V
	for {
		lease, err := c.config.locker.Lock(ctx, c.namespace, key, c.config.leaseTTL)
		if err == nil {
			if isNil(lease) {
				return nil, zero, false, false, ErrNilLease
			}
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

func (c *PullThroughCache[K, V]) loadAndStore(ctx context.Context, key K) (V, bool, error) {
	value, found, err := c.callLoader(ctx, key)
	if err != nil {
		var zero V
		return zero, false, err
	}
	if err := c.storeLoaded(ctx, key, value, found); err != nil {
		return value, found, err
	}
	return value, found, nil
}

func (c *PullThroughCache[K, V]) loadAndStoreWithLease(ctx context.Context, key K, lease Lease) (V, bool, error) {
	loadCtx, cancelLoad := context.WithCancel(ctx)
	guard := c.guardLease(ctx, lease, cancelLoad)
	value, found, loadErr := c.callLoader(loadCtx, key)

	lossErr := guard.Err()
	var storeErr error
	if loadErr == nil && lossErr == nil {
		storeErr = c.storeLoaded(loadCtx, key, value, found)
		lossErr = guard.Err()
	}
	lossErr = errors.Join(lossErr, guard.Stop())
	cancelLoad()
	releaseErr := c.release(ctx, lease)

	if lossErr != nil {
		var zero V
		return zero, false, errors.Join(ErrFillLeaseLost, lossErr, loadErr, releaseErr)
	}
	if err := errors.Join(loadErr, storeErr, releaseErr); err != nil {
		return value, found, err
	}
	return value, found, nil
}

func (c *PullThroughCache[K, V]) storeLoaded(ctx context.Context, key K, value V, found bool) error {
	if found {
		return c.cache.Set(ctx, key, value)
	}
	return c.storeNegative(ctx, key)
}

func (c *PullThroughCache[K, V]) storeNegative(ctx context.Context, key K) error {
	expiresAt := time.Now().Add(c.config.negativeTTL)
	if access, ok := c.cache.(EntryCache[K, V]); ok {
		var zero V
		return access.SetEntry(ctx, key, Entry[V]{Value: zero, Found: false, ExpiresAt: expiresAt})
	}
	return nil
}

type leaseGuard struct {
	stop       chan struct{}
	done       chan struct{}
	loss       chan error
	observable <-chan struct{}
	cancel     context.CancelFunc
	lossOnce   sync.Once
	stopOnce   sync.Once
}

func (c *PullThroughCache[K, V]) guardLease(
	ctx context.Context,
	lease Lease,
	cancel context.CancelFunc,
) *leaseGuard {
	guard := &leaseGuard{
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		loss:   make(chan error, 1),
		cancel: cancel,
	}
	if observable, ok := lease.(ObservableLease); ok {
		guard.observable = observable.Done()
	}
	go guard.renew(ctx, lease, c.config.leaseTTL)
	return guard
}

func (g *leaseGuard) renew(ctx context.Context, lease Lease, ttl time.Duration) {
	defer close(g.done)
	defer func() {
		if recovered := recover(); recovered != nil {
			g.lose(fmt.Errorf("cache: lease refresh panicked: %v", recovered))
		}
	}()
	interval := ttl / 2
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.observable:
			g.lose(ErrLeaseLost)
			return
		case <-g.stop:
			g.observeLoss()
			return
		case <-ticker.C:
			if err := lease.Refresh(ctx, ttl); err != nil {
				if ctx.Err() == nil {
					g.lose(err)
				}
				return
			}
		}
	}
}

func (g *leaseGuard) observeLoss() {
	if g.observable == nil {
		return
	}
	select {
	case <-g.observable:
		g.lose(ErrLeaseLost)
	default:
	}
}

func (g *leaseGuard) lose(err error) {
	g.lossOnce.Do(func() {
		g.loss <- err
		g.cancel()
	})
}

func (g *leaseGuard) Err() error {
	g.observeLoss()
	select {
	case err := <-g.loss:
		g.loss <- err
		return err
	default:
		return nil
	}
}

func (g *leaseGuard) Stop() error {
	g.stopOnce.Do(func() { close(g.stop) })
	<-g.done
	return g.Err()
}

func (c *PullThroughCache[K, V]) release(ctx context.Context, lease Lease) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.leaseTTL)
	defer cancel()
	return lease.Release(releaseCtx)
}

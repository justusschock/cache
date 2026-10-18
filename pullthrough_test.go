package cache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMemoryCache(t *testing.T) cache.Cache[string, string] {
	t.Helper()
	store := memory.New[string, string]()
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)
	return c
}

// FR-CACHE-09
func TestPullThroughLoadsAndCachesMiss(t *testing.T) {
	base := newMemoryCache(t)
	var calls atomic.Int32
	pull, err := cache.NewPullThrough("users", base, func(context.Context, string) (string, bool, error) {
		calls.Add(1)
		return "loaded", true, nil
	})
	require.NoError(t, err)

	for range 2 {
		value, found, getErr := pull.Get(context.Background(), "alice")
		require.NoError(t, getErr)
		assert.True(t, found)
		assert.Equal(t, "loaded", value)
	}
	assert.Equal(t, int32(1), calls.Load())
}

// FR-CACHE-10
func TestPullThroughCachesAuthoritativeMiss(t *testing.T) {
	base := newMemoryCache(t)
	var calls atomic.Int32
	pull, err := cache.NewPullThrough(
		"users",
		base,
		func(context.Context, string) (string, bool, error) {
			calls.Add(1)
			return "", false, nil
		},
		cache.WithNegativeTTL[string](time.Minute),
	)
	require.NoError(t, err)

	for range 2 {
		value, found, getErr := pull.Get(context.Background(), "missing")
		require.NoError(t, getErr)
		assert.False(t, found)
		assert.Empty(t, value)
	}
	assert.Equal(t, int32(1), calls.Load())
}

// FR-CACHE-10
func TestPullThroughDoesNotCacheLoaderErrors(t *testing.T) {
	base := newMemoryCache(t)
	loaderErr := errors.New("database unavailable")
	var calls atomic.Int32
	pull, err := cache.NewPullThrough("users", base, func(context.Context, string) (string, bool, error) {
		calls.Add(1)
		return "", false, loaderErr
	})
	require.NoError(t, err)

	for range 2 {
		_, _, getErr := pull.Get(context.Background(), "alice")
		assert.ErrorIs(t, getErr, loaderErr)
	}
	assert.Equal(t, int32(2), calls.Load())
}

// FR-CACHE-09
func TestPullThroughSuppressesConcurrentLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		pull, err := cache.NewPullThrough("users", base, func(context.Context, string) (string, bool, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return "loaded", true, nil
		})
		require.NoError(t, err)

		const workers = 32
		results := make(chan error, workers)
		for range workers {
			goReport(results, func() error {
				value, found, getErr := pull.Get(context.Background(), "alice")
				if getErr != nil {
					return getErr
				}
				if !found || value != "loaded" {
					return errors.New("unexpected pull-through result")
				}
				return nil
			})
		}
		<-started
		close(release)
		for range workers {
			assert.NoError(t, <-results)
		}
		assert.Equal(t, int32(1), calls.Load())
	})
}

type fakeLease struct {
	refreshes  atomic.Int32
	releases   atomic.Int32
	releaseErr error
}

func (l *fakeLease) Refresh(context.Context, time.Duration) error {
	l.refreshes.Add(1)
	return nil
}

func (l *fakeLease) Release(context.Context) error {
	l.releases.Add(1)
	return l.releaseErr
}

type fakeLocker struct {
	lease *fakeLease
	err   error
}

type nilLeaseLocker struct{}

func (nilLeaseLocker) Lock(context.Context, string, string, time.Duration) (cache.Lease, error) {
	return nil, nil
}

type plainCache struct {
	getCalls atomic.Int32
}

func (c *plainCache) Get(context.Context, string) (string, bool, error) {
	c.getCalls.Add(1)
	return "", false, nil
}

func (*plainCache) Set(context.Context, string, string) error { return nil }

func (*plainCache) SetWithTTL(context.Context, string, string, time.Duration) error { return nil }

func (*plainCache) Invalidate(context.Context, string) error { return nil }

func (*plainCache) InvalidateAll(context.Context) error { return nil }

type recordingEntryCache struct {
	setCalls atomic.Int32
}

func (*recordingEntryCache) Get(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (c *recordingEntryCache) Set(context.Context, string, string) error {
	c.setCalls.Add(1)
	return nil
}

func (c *recordingEntryCache) SetWithTTL(context.Context, string, string, time.Duration) error {
	c.setCalls.Add(1)
	return nil
}

func (*recordingEntryCache) Invalidate(context.Context, string) error { return nil }

func (*recordingEntryCache) InvalidateAll(context.Context) error { return nil }

func (*recordingEntryCache) GetEntry(context.Context, string) (cache.Entry[string], bool, error) {
	return cache.Entry[string]{}, false, nil
}

func (c *recordingEntryCache) SetEntry(context.Context, string, cache.Entry[string]) error {
	c.setCalls.Add(1)
	return nil
}

type controllableLease struct {
	lost     chan struct{}
	releases atomic.Int32
}

func (*controllableLease) Refresh(context.Context, time.Duration) error { return nil }

func (l *controllableLease) Release(context.Context) error {
	l.releases.Add(1)
	return nil
}

func (l *controllableLease) Done() <-chan struct{} { return l.lost }

type controllableLocker struct {
	lease cache.Lease
}

type failingRefreshLease struct {
	once      sync.Once
	refreshed chan struct{}
	releases  atomic.Int32
}

func (l *failingRefreshLease) Refresh(context.Context, time.Duration) error {
	l.once.Do(func() { close(l.refreshed) })
	return cache.ErrLeaseLost
}

func (l *failingRefreshLease) Release(context.Context) error {
	l.releases.Add(1)
	return nil
}

func (l *controllableLocker) Lock(context.Context, string, string, time.Duration) (cache.Lease, error) {
	return l.lease, nil
}

func (l *fakeLocker) Lock(context.Context, string, string, time.Duration) (cache.Lease, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.lease, nil
}

// FR-CACHE-11
func TestPullThroughRenewsAndReleasesDistributedLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		lease := &fakeLease{}
		locker := &fakeLocker{lease: lease}
		pull, err := cache.NewPullThrough(
			"users",
			base,
			func(context.Context, string) (string, bool, error) {
				time.Sleep(35 * time.Millisecond)
				return "loaded", true, nil
			},
			cache.WithLocker[string](locker, 20*time.Millisecond),
		)
		require.NoError(t, err)

		value, found, err := pull.Get(context.Background(), "alice")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "loaded", value)
		// The lease is refreshed every half TTL (10ms) while the 35ms load runs.
		assert.Equal(t, int32(3), lease.refreshes.Load())
		assert.Equal(t, int32(1), lease.releases.Load())
	})
}

// FR-CACHE-11
func TestPullThroughDoesNotPublishFillAfterLeaseLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := &recordingEntryCache{}
		lease := &controllableLease{lost: make(chan struct{})}
		started := make(chan struct{})
		pull, err := cache.NewPullThrough(
			"users",
			base,
			func(ctx context.Context, _ string) (string, bool, error) {
				close(started)
				<-lease.lost
				<-ctx.Done()
				return "stale", true, ctx.Err()
			},
			cache.WithLocker[string](&controllableLocker{lease: lease}, time.Second),
		)
		require.NoError(t, err)

		result := make(chan error, 1)
		go func() {
			_, _, getErr := pull.Get(context.Background(), "alice")
			result <- getErr
		}()
		<-started
		close(lease.lost)

		require.ErrorIs(t, <-result, cache.ErrFillLeaseLost)
		assert.Zero(t, base.setCalls.Load())
		assert.Equal(t, int32(1), lease.releases.Load())
	})
}

// FR-CACHE-11
func TestPullThroughDoesNotPublishFillAfterRenewalFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := &recordingEntryCache{}
		lease := &failingRefreshLease{refreshed: make(chan struct{})}
		pull, err := cache.NewPullThrough(
			"users",
			base,
			func(ctx context.Context, _ string) (string, bool, error) {
				<-ctx.Done()
				return "stale", true, ctx.Err()
			},
			cache.WithLocker[string](&controllableLocker{lease: lease}, 2*time.Millisecond),
		)
		require.NoError(t, err)

		_, _, err = pull.Get(context.Background(), "alice")
		require.ErrorIs(t, err, cache.ErrFillLeaseLost)
		assert.Zero(t, base.setCalls.Load())
		assert.Equal(t, int32(1), lease.releases.Load())
	})
}

// FR-CACHE-10
func TestPullThroughCustomCacheDoesNotRetainLocalNegativeEntries(t *testing.T) {
	base := &plainCache{}
	var loads atomic.Int32
	pull, err := cache.NewPullThrough("users", base, func(context.Context, string) (string, bool, error) {
		loads.Add(1)
		return "", false, nil
	})
	require.NoError(t, err)

	for range 2 {
		_, found, getErr := pull.Get(context.Background(), "missing")
		require.NoError(t, getErr)
		assert.False(t, found)
	}
	assert.Equal(t, int32(2), loads.Load())
}

// FR-CACHE-11
func TestPullThroughLockerFailurePolicy(t *testing.T) {
	base := newMemoryCache(t)
	lockErr := errors.New("lock unavailable")
	locker := &fakeLocker{err: lockErr}
	var calls atomic.Int32
	loader := func(context.Context, string) (string, bool, error) {
		calls.Add(1)
		return "loaded", true, nil
	}

	pull, err := cache.NewPullThrough("users", base, loader, cache.WithLocker[string](locker, time.Second))
	require.NoError(t, err)
	_, _, err = pull.Get(context.Background(), "alice")
	assert.ErrorIs(t, err, lockErr)
	assert.Zero(t, calls.Load())

	pull, err = cache.NewPullThrough(
		"users",
		base,
		loader,
		cache.WithLocker[string](locker, time.Second),
		cache.WithLoadOnLockError[string](),
	)
	require.NoError(t, err)
	value, found, err := pull.Get(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "loaded", value)
	assert.Equal(t, int32(1), calls.Load())
}

// FR-CACHE-22
func TestNewPullThroughRejectsInvalidConfiguration(t *testing.T) {
	base := newMemoryCache(t)
	loader := func(context.Context, string) (string, bool, error) { return "", false, nil }

	_, err := cache.NewPullThrough("", base, loader)
	assert.ErrorIs(t, err, cache.ErrEmptyNamespace)
	_, err = cache.NewPullThrough[string, string]("users", nil, loader)
	assert.ErrorIs(t, err, cache.ErrNilCache)
	_, err = cache.NewPullThrough("users", base, nil)
	assert.ErrorIs(t, err, cache.ErrNilLoader)
	_, err = cache.NewPullThrough("users", base, loader, cache.WithNegativeTTL[string](0))
	assert.ErrorIs(t, err, cache.ErrInvalidTTL)
}

// FR-CACHE-11
func TestPullThroughRejectsNilSuccessfulLease(t *testing.T) {
	base := newMemoryCache(t)
	loader := func(context.Context, string) (string, bool, error) { return "loaded", true, nil }
	for name, locker := range map[string]cache.Locker[string]{
		"nil":       nilLeaseLocker{},
		"typed nil": &fakeLocker{},
	} {
		t.Run(name, func(t *testing.T) {
			pull, err := cache.NewPullThrough(
				"users", base, loader, cache.WithLocker[string](locker, time.Minute),
			)
			require.NoError(t, err)

			_, _, err = pull.Get(context.Background(), "alice-"+name)
			assert.ErrorIs(t, err, cache.ErrNilLease)
		})
	}
}

type stagedEntryCache struct {
	reads    atomic.Int32
	thirdErr error
}

func (c *stagedEntryCache) GetEntry(context.Context, string) (cache.Entry[string], bool, error) {
	if c.reads.Add(1) < 3 {
		return cache.Entry[string]{}, false, nil
	}
	if c.thirdErr != nil {
		return cache.Entry[string]{}, false, c.thirdErr
	}
	return cache.Entry[string]{Value: "loaded", Found: true, ExpiresAt: time.Now().Add(time.Minute)}, true, nil
}

func (c *stagedEntryCache) Get(ctx context.Context, key string) (string, bool, error) {
	entry, present, err := c.GetEntry(ctx, key)
	return entry.Value, present && entry.Found, err
}

func (*stagedEntryCache) Set(context.Context, string, string) error { return nil }
func (*stagedEntryCache) SetWithTTL(context.Context, string, string, time.Duration) error {
	return nil
}
func (*stagedEntryCache) SetEntry(context.Context, string, cache.Entry[string]) error { return nil }
func (*stagedEntryCache) Invalidate(context.Context, string) error                    { return nil }
func (*stagedEntryCache) InvalidateAll(context.Context) error                         { return nil }

// FR-CACHE-11
func TestPullThroughPostAcquireHitPropagatesReleaseError(t *testing.T) {
	base := &stagedEntryCache{}
	releaseErr := errors.New("release failed")
	lease := &fakeLease{releaseErr: releaseErr}
	pull, err := cache.NewPullThrough(
		"users",
		base,
		func(context.Context, string) (string, bool, error) {
			return "", false, errors.New("loader must not run")
		},
		cache.WithLocker[string](&fakeLocker{lease: lease}, time.Minute),
	)
	require.NoError(t, err)

	value, found, err := pull.Get(context.Background(), "alice")
	assert.Equal(t, "loaded", value)
	assert.True(t, found)
	assert.ErrorIs(t, err, releaseErr)
	assert.Equal(t, int32(1), lease.releases.Load())
}

// FR-CACHE-11
func TestPullThroughPostAcquireStrictLookupErrorStillReleases(t *testing.T) {
	lookupErr := errors.New("lookup failed")
	base := &stagedEntryCache{thirdErr: lookupErr}
	lease := &fakeLease{}
	pull, err := cache.NewPullThrough(
		"users",
		base,
		func(context.Context, string) (string, bool, error) {
			return "", false, errors.New("loader must not run")
		},
		cache.WithLocker[string](&fakeLocker{lease: lease}, time.Minute),
		cache.WithStrictPullThroughReads[string](),
	)
	require.NoError(t, err)

	_, _, err = pull.Get(context.Background(), "alice")
	assert.ErrorIs(t, err, lookupErr)
	assert.Equal(t, int32(1), lease.releases.Load())
}

type contendedLocker struct {
	calls atomic.Int32
}

func (l *contendedLocker) Lock(context.Context, string, string, time.Duration) (cache.Lease, error) {
	l.calls.Add(1)
	return nil, cache.ErrLockNotAcquired
}

// FR-CACHE-11
func TestPullThroughPollsContendedLockUntilAnotherFillLands(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		locker := &contendedLocker{}
		var loads atomic.Int32
		pull, err := cache.NewPullThrough(
			"users",
			base,
			func(context.Context, string) (string, bool, error) {
				loads.Add(1)
				return "local", true, nil
			},
			cache.WithLocker[string](locker, time.Minute),
			cache.WithLockPollInterval[string](10*time.Millisecond),
		)
		require.NoError(t, err)

		go func() {
			// Another process holding the lease completes its fill.
			time.Sleep(35 * time.Millisecond)
			assert.NoError(t, base.Set(context.Background(), "alice", "remote"))
		}()

		start := time.Now()
		value, found, err := pull.Get(context.Background(), "alice")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "remote", value)
		assert.Zero(t, loads.Load())
		assert.Equal(t, int32(5), locker.calls.Load())
		assert.Equal(t, 40*time.Millisecond, time.Since(start))
	})
}

// FR-CACHE-11
func TestPullThroughContendedLockHonoursContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		pull, err := cache.NewPullThrough(
			"users",
			base,
			func(context.Context, string) (string, bool, error) { return "local", true, nil },
			cache.WithLocker[string](&contendedLocker{}, time.Minute),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, err = pull.Get(ctx, "alice")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// FR-CACHE-10
func TestPullThroughNegativeEntriesExpireAfterNegativeTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		var loads atomic.Int32
		pull, err := cache.NewPullThrough(
			"users",
			base,
			func(context.Context, string) (string, bool, error) {
				loads.Add(1)
				return "", false, nil
			},
			cache.WithNegativeTTL[string](10*time.Second),
		)
		require.NoError(t, err)

		for range 2 {
			_, found, getErr := pull.Get(context.Background(), "missing")
			require.NoError(t, getErr)
			assert.False(t, found)
		}
		assert.Equal(t, int32(1), loads.Load())

		time.Sleep(10 * time.Second)
		_, found, err := pull.Get(context.Background(), "missing")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, int32(2), loads.Load())
	})
}

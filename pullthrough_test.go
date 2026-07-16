package cache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMemoryCache(t *testing.T) cache.Cache[string, string] {
	t.Helper()
	c, err := cache.New("users", time.Minute, memory.New[string, string]())
	require.NoError(t, err)
	return c
}

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

func TestPullThroughSuppressesConcurrentLoads(t *testing.T) {
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
		cachetest.Go(results, func() error {
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
}

type fakeLease struct {
	refreshes atomic.Int32
	releases  atomic.Int32
}

func (l *fakeLease) Refresh(context.Context, time.Duration) error {
	l.refreshes.Add(1)
	return nil
}

func (l *fakeLease) Release(context.Context) error {
	l.releases.Add(1)
	return nil
}

type fakeLocker struct {
	lease *fakeLease
	err   error
}

func (l *fakeLocker) Lock(context.Context, string, string, time.Duration) (cache.Lease, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.lease, nil
}

func TestPullThroughRenewsAndReleasesDistributedLease(t *testing.T) {
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
	assert.GreaterOrEqual(t, lease.refreshes.Load(), int32(1))
	assert.Equal(t, int32(1), lease.releases.Load())
}

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

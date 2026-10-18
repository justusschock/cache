package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	cache "github.com/justusschock/cache"
	coordination "github.com/justusschock/distributed-coordination"
	coordinationmemory "github.com/justusschock/distributed-coordination/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type coordinationBackend struct {
	request coordination.AcquireRequest
	lease   coordination.Lease
	err     error
}

var _ cache.Locker[string] = (*cache.CoordinationLocker[string])(nil)

func (b *coordinationBackend) Acquire(
	_ context.Context,
	request coordination.AcquireRequest,
) (coordination.Lease, error) {
	b.request = request
	return b.lease, b.err
}

type coordinationLease struct {
	done     chan struct{}
	renewTTL time.Duration
	released bool
}

func (*coordinationLease) Info() coordination.LeaseInfo { return coordination.LeaseInfo{} }

func (l *coordinationLease) Done() <-chan struct{} { return l.done }

func (l *coordinationLease) Renew(_ context.Context, ttl time.Duration) (coordination.LeaseInfo, error) {
	l.renewTTL = ttl
	return coordination.LeaseInfo{}, nil
}

func (l *coordinationLease) Release(context.Context) error {
	l.released = true
	return nil
}

// FR-CACHE-12, FR-CACHE-22
func TestNewCoordinationLockerRejectsInvalidConfiguration(t *testing.T) {
	key := func(string, string) (string, error) { return "key", nil }
	backend := &coordinationBackend{}

	_, err := cache.NewCoordinationLocker[string](nil, "worker", key)
	assert.ErrorIs(t, err, cache.ErrNilCoordinationBackend)
	_, err = cache.NewCoordinationLocker[string](backend, "", key)
	assert.ErrorIs(t, err, cache.ErrEmptyCoordinationOwner)
	_, err = cache.NewCoordinationLocker[string](backend, "worker", nil)
	assert.ErrorIs(t, err, cache.ErrNilFillKeyFunc)
}

// FR-CACHE-12
func TestCoordinationLockerMapsLeaseOperations(t *testing.T) {
	underlying := &coordinationLease{done: make(chan struct{})}
	backend := &coordinationBackend{lease: underlying}
	locker, err := cache.NewCoordinationLocker(
		backend,
		"worker-1",
		func(namespace string, key string) (string, error) { return namespace + "/" + key, nil },
	)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), "users", "alice", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, coordination.AcquireRequest{
		Key: "users/alice", Owner: "worker-1", TTL: time.Minute, Wait: true,
	}, backend.request)

	observable, ok := lease.(cache.ObservableLease)
	require.True(t, ok)
	assert.Equal(t, (<-chan struct{})(underlying.done), observable.Done())
	require.NoError(t, lease.Refresh(context.Background(), 2*time.Minute))
	assert.Equal(t, 2*time.Minute, underlying.renewTTL)
	require.NoError(t, lease.Release(context.Background()))
	assert.True(t, underlying.released)
}

// FR-CACHE-12
func TestCoordinationLockerMapsContention(t *testing.T) {
	backend := &coordinationBackend{err: coordination.ErrNotAcquired}
	locker, err := cache.NewCoordinationLocker(
		backend,
		"worker-1",
		func(string, string) (string, error) { return "users/alice", nil },
	)
	require.NoError(t, err)

	_, err = locker.Lock(context.Background(), "users", "alice", time.Minute)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
}

// FR-CACHE-12
func TestCoordinationLockerWorksWithMemoryBackend(t *testing.T) {
	backend := coordinationmemory.NewBackend(nil)
	locker, err := cache.NewCoordinationLocker(
		backend,
		"worker-1",
		func(namespace string, key string) (string, error) { return namespace + "/" + key, nil },
	)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), "users", "alice", time.Minute)
	require.NoError(t, err)
	_, err = backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key: "users/alice", Owner: "worker-2", TTL: time.Minute, Wait: false,
	})
	assert.ErrorIs(t, err, coordination.ErrNotAcquired)
	require.NoError(t, lease.Release(context.Background()))

	second, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key: "users/alice", Owner: "worker-2", TTL: time.Minute, Wait: false,
	})
	require.NoError(t, err)
	require.NoError(t, second.Release(context.Background()))
}

// FR-CACHE-12
func TestCoordinationLockerPreservesKeyErrors(t *testing.T) {
	wantErr := errors.New("cannot encode key")
	locker, err := cache.NewCoordinationLocker(
		&coordinationBackend{},
		"worker-1",
		func(string, string) (string, error) { return "", wantErr },
	)
	require.NoError(t, err)

	_, err = locker.Lock(context.Background(), "users", "alice", time.Minute)
	assert.ErrorIs(t, err, wantErr)
}

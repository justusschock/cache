package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockerAcquireRefreshAndRelease(t *testing.T) {
	locker := memory.NewLocker[string]()
	lease, err := locker.Lock(context.Background(), "users", "alice", 20*time.Millisecond)
	require.NoError(t, err)

	_, err = locker.Lock(context.Background(), "users", "alice", time.Minute)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Refresh(context.Background(), time.Minute))
	time.Sleep(30 * time.Millisecond)
	_, err = locker.Lock(context.Background(), "users", "alice", time.Minute)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)

	require.NoError(t, lease.Release(context.Background()))
	_, err = locker.Lock(context.Background(), "users", "alice", time.Minute)
	assert.NoError(t, err)
}

func TestLockerExpiredLeaseCanBeReplaced(t *testing.T) {
	locker := memory.NewLocker[string]()
	lease, err := locker.Lock(context.Background(), "users", "alice", 5*time.Millisecond)
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)

	replacement, err := locker.Lock(context.Background(), "users", "alice", time.Minute)
	require.NoError(t, err)
	assert.ErrorIs(t, lease.Refresh(context.Background(), time.Minute), cache.ErrLeaseLost)
	assert.NoError(t, replacement.Release(context.Background()))
}

func TestLockerReleaseRejectsNaturalExpiry(t *testing.T) {
	locker := memory.NewLocker[string]()
	lease, err := locker.Lock(context.Background(), "users", "alice", time.Millisecond)
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	assert.ErrorIs(t, lease.Release(context.Background()), cache.ErrLeaseLost)
}

func TestLockerAllowsOnlyOneConcurrentHolder(t *testing.T) {
	locker := memory.NewLocker[string]()
	const workers = 32
	results := make(chan error, workers)
	for range workers {
		go func() {
			_, err := locker.Lock(context.Background(), "users", "alice", time.Minute)
			results <- err
		}()
	}

	acquired := 0
	contended := 0
	for range workers {
		err := <-results
		switch {
		case err == nil:
			acquired++
		case errors.Is(err, cache.ErrLockNotAcquired):
			contended++
		default:
			assert.NoError(t, err)
		}
	}
	assert.Equal(t, 1, acquired)
	assert.Equal(t, workers-1, contended)
}

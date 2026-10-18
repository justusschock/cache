//go:build integration

package redis_test

import (
	"context"
	"os"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	rediscache "github.com/justusschock/go-cache/backend/redis"
	"github.com/justusschock/go-cache/cachetest"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FR-CACHE-06, FR-CACHE-19
func TestStoreIntegrationContract(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Ping(context.Background()).Err())

	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, err := rediscache.New[string, int](client)
		require.NoError(t, err)
		return store
	})
	cachetest.RunBatchStoreContract(t, func(t *testing.T) cache.BatchStore[string, int] {
		store, err := rediscache.New[string, int](client)
		require.NoError(t, err)
		return store
	})
}

// FR-CACHE-20
func TestLockerIntegration(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Ping(context.Background()).Err())
	locker, err := rediscache.NewLocker[string](client)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	require.NoError(t, err)
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Refresh(context.Background(), time.Second))
	require.NoError(t, lease.Release(context.Background()))
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.NoError(t, err)
}

// FR-CACHE-15
func TestInvalidationBusIntegration(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Ping(context.Background()).Err())

	cachetest.RunInvalidationBusContract(t, func(t *testing.T) cache.InvalidationBus[string] {
		bus, err := rediscache.NewInvalidationBus[string](client)
		require.NoError(t, err)
		return bus
	}, cache.InvalidationEvent[string]{Namespace: t.Name(), Kind: cache.InvalidateKey, Key: "alice", Generation: 3})
}

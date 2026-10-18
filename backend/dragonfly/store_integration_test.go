//go:build integration

package dragonfly_test

import (
	"context"
	"os"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/dragonfly"
	"github.com/justusschock/go-cache/cachetest"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIntegrationContract(t *testing.T) {
	address := os.Getenv("DRAGONFLY_ADDR")
	if address == "" {
		address = "127.0.0.1:6380"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Ping(context.Background()).Err())

	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, err := dragonfly.New[string, int](client)
		require.NoError(t, err)
		return store
	})
	cachetest.RunBatchStoreContract(t, func(t *testing.T) cache.BatchStore[string, int] {
		store, err := dragonfly.New[string, int](client)
		require.NoError(t, err)
		return store
	})
}

func TestLockerIntegration(t *testing.T) {
	address := os.Getenv("DRAGONFLY_ADDR")
	if address == "" {
		address = "127.0.0.1:6380"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Ping(context.Background()).Err())
	locker, err := dragonfly.NewLocker[string](client)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	require.NoError(t, err)
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Release(context.Background()))
}

func TestInvalidationBusIntegration(t *testing.T) {
	address := os.Getenv("DRAGONFLY_ADDR")
	if address == "" {
		address = "127.0.0.1:6380"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Ping(context.Background()).Err())

	cachetest.RunInvalidationBusContract(t, func(t *testing.T) cache.InvalidationBus[string] {
		bus, err := dragonfly.NewInvalidationBus[string](client)
		require.NoError(t, err)
		return bus
	}, cache.InvalidationEvent[string]{Namespace: t.Name(), Kind: cache.InvalidateKey, Key: "alice"})
}

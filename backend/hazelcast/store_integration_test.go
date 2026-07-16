//go:build integration

package hazelcast_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	cache "github.com/justusschock/go-cache"
	hazelcastcache "github.com/justusschock/go-cache/backend/hazelcast"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIntegrationContract(t *testing.T) {
	address := os.Getenv("HAZELCAST_ADDR")
	if address == "" {
		address = "127.0.0.1:5701"
	}
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses(address)
	client, err := hazelcast.StartNewClientWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Shutdown(context.Background()))
	})

	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, storeErr := hazelcastcache.New[string, int](context.Background(), client)
		require.NoError(t, storeErr)
		return store
	})
}

func TestLockerIntegration(t *testing.T) {
	address := os.Getenv("HAZELCAST_ADDR")
	if address == "" {
		address = "127.0.0.1:5701"
	}
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses(address)
	client, err := hazelcast.StartNewClientWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Shutdown(context.Background()))
	})
	locker, err := hazelcastcache.NewLocker[string](context.Background(), client)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	require.NoError(t, err)
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Refresh(context.Background(), time.Second))
	require.NoError(t, lease.Release(context.Background()))
}

func TestInvalidationBusIntegration(t *testing.T) {
	address := os.Getenv("HAZELCAST_ADDR")
	if address == "" {
		address = "127.0.0.1:5701"
	}
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses(address)
	client, err := hazelcast.StartNewClientWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Shutdown(context.Background()))
	})

	cachetest.RunInvalidationBusContract(t, func(t *testing.T) cache.InvalidationBus[string] {
		bus, busErr := hazelcastcache.NewInvalidationBus[string](context.Background(), client)
		require.NoError(t, busErr)
		return bus
	}, cache.InvalidationEvent[string]{Namespace: t.Name(), Kind: cache.InvalidateNamespace})
}

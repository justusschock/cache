//go:build integration

package hazelcast_test

import (
	"context"
	"os"
	"testing"

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

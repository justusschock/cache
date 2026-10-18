//go:build integration

package valkey_test

import (
	"context"
	"os"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	valkeycache "github.com/justusschock/go-cache/backend/valkey"
	"github.com/justusschock/go-cache/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeyclient "github.com/valkey-io/valkey-go"
)

// FR-CACHE-06, FR-CACHE-19
func TestStoreIntegrationContract(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		address = "127.0.0.1:6381"
	}
	client, err := valkeyclient.NewClient(valkeyclient.ClientOption{InitAddress: []string{address}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.NoError(t, client.Do(context.Background(), client.B().Ping().Build()).Error())

	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, storeErr := valkeycache.New[string, int](client)
		require.NoError(t, storeErr)
		return store
	})
	cachetest.RunBatchStoreContract(t, func(t *testing.T) cache.BatchStore[string, int] {
		store, storeErr := valkeycache.New[string, int](client)
		require.NoError(t, storeErr)
		return store
	})
}

// FR-CACHE-20
func TestLockerIntegration(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		address = "127.0.0.1:6381"
	}
	client, err := valkeyclient.NewClient(valkeyclient.ClientOption{InitAddress: []string{address}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	locker, err := valkeycache.NewLocker[string](client)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	require.NoError(t, err)
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Refresh(context.Background(), time.Second))
	require.NoError(t, lease.Release(context.Background()))
}

// FR-CACHE-15
func TestInvalidationBusIntegration(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		address = "127.0.0.1:6381"
	}
	client, err := valkeyclient.NewClient(valkeyclient.ClientOption{InitAddress: []string{address}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.NoError(t, client.Do(context.Background(), client.B().Ping().Build()).Error())

	cachetest.RunInvalidationBusContract(t, func(t *testing.T) cache.InvalidationBus[string] {
		bus, busErr := valkeycache.NewInvalidationBus[string](client)
		require.NoError(t, busErr)
		return bus
	}, cache.InvalidationEvent[string]{Namespace: t.Name(), Kind: cache.InvalidateNamespace, Generation: 4})
}

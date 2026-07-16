//go:build integration

package memcached_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memcached"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIntegrationContract(t *testing.T) {
	address := os.Getenv("MEMCACHED_ADDR")
	if address == "" {
		address = "127.0.0.1:11211"
	}
	client := memcache.New(address)

	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, err := memcached.New[string, int](client)
		require.NoError(t, err)
		return store
	})
}

func TestLockerIntegration(t *testing.T) {
	address := os.Getenv("MEMCACHED_ADDR")
	if address == "" {
		address = "127.0.0.1:11211"
	}
	client := memcache.New(address)
	locker, err := memcached.NewLocker[string](client)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	require.NoError(t, err)
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Refresh(context.Background(), time.Second))
	require.NoError(t, lease.Release(context.Background()))
}

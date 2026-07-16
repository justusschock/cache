//go:build integration

package memcached_test

import (
	"os"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memcached"
	"github.com/justusschock/go-cache/internal/cachetest"
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

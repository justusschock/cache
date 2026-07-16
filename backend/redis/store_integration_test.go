//go:build integration

package redis_test

import (
	"context"
	"os"
	"testing"

	cache "github.com/justusschock/go-cache"
	rediscache "github.com/justusschock/go-cache/backend/redis"
	"github.com/justusschock/go-cache/internal/cachetest"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
}

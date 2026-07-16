//go:build integration

package dragonfly_test

import (
	"context"
	"os"
	"testing"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/dragonfly"
	"github.com/justusschock/go-cache/internal/cachetest"
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
}

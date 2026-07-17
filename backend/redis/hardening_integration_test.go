//go:build integration

package redis

import (
	"context"
	"os"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidationBusFiltersMismatchedNamespacePayloads(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { assert.NoError(t, client.Close()) })
	bus, err := NewInvalidationBus[string](client)
	require.NoError(t, err)

	namespace := t.Name()
	wrong := cache.InvalidationEvent[string]{Namespace: namespace + "-wrong", Kind: cache.InvalidateNamespace}
	right := cache.InvalidationEvent[string]{Namespace: namespace, Kind: cache.InvalidateNamespace}
	wrongPayload, err := wire.EncodeInvalidation(wrong, cache.DefaultCodec[string]())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received := make(chan cache.InvalidationEvent[string], 1)
	result := make(chan error, 1)
	go func() {
		result <- bus.Subscribe(ctx, namespace, func(_ context.Context, event cache.InvalidationEvent[string]) error {
			received <- event
			return nil
		})
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		require.NoError(t, client.Publish(ctx, invalidationChannel(namespace), wrongPayload).Err())
		require.NoError(t, bus.Publish(ctx, right))
		select {
		case event := <-received:
			assert.Equal(t, right, event)
			cancel()
			assert.ErrorIs(t, <-result, context.Canceled)
			return
		case <-ticker.C:
		case <-ctx.Done():
			require.NoError(t, context.Cause(ctx))
		}
	}
}

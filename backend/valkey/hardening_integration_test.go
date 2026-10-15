//go:build integration

package valkey

import (
	"context"
	"os"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeyclient "github.com/valkey-io/valkey-go"
)

func TestInvalidationBusFiltersMismatchedNamespacePayloads(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		address = "127.0.0.1:6381"
	}
	client, err := valkeyclient.NewClient(valkeyclient.ClientOption{InitAddress: []string{address}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
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
		command := client.B().Publish().Channel(invalidationChannel(namespace)).Message(string(wrongPayload)).Build()
		require.NoError(t, client.Do(ctx, command).Error())
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

func TestNoExpiryEntriesHaveNoNativeTTL(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		address = "127.0.0.1:6381"
	}
	client, err := valkeyclient.NewClient(valkeyclient.ClientOption{InitAddress: []string{address}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	store, err := New[string, int](client)
	require.NoError(t, err)
	ctx := context.Background()
	namespace := t.Name()

	require.NoError(t, store.Set(ctx, namespace, "single", cache.Entry[int]{Value: 1, Found: true}))
	require.NoError(t, store.SetMany(ctx, namespace, []string{"batch", "expiring"}, []cache.Entry[int]{
		{Value: 2, Found: true},
		{Value: 3, Found: true, ExpiresAt: time.Now().Add(time.Hour)},
	}))
	for key, persistent := range map[string]bool{"single": true, "batch": true, "expiring": false} {
		entryKey, _, err := store.keys(namespace, key)
		require.NoError(t, err)
		ttl, err := client.Do(ctx, client.B().Pttl().Key(entryKey).Build()).AsInt64()
		require.NoError(t, err)
		if persistent {
			assert.Equal(t, int64(-1), ttl, key)
		} else {
			assert.Positive(t, ttl, key)
		}
		require.NoError(t, client.Do(ctx, client.B().Del().Key(entryKey).Build()).Error())
	}
}

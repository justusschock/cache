package cache_test

import (
	"context"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayeredInvalidationBusEvictsOtherWorkersUpperLayers(t *testing.T) {
	bus := memory.NewInvalidationBus[string]()
	lower := memory.New[string, string]()
	upperA := memory.New[string, string]()
	upperB := memory.New[string, string]()
	layeredA, err := cache.NewLayeredWithInvalidationBus(
		[]cache.Store[string, string]{upperA, lower},
		bus,
	)
	require.NoError(t, err)
	layeredB, err := cache.NewLayeredWithInvalidationBus(
		[]cache.Store[string, string]{upperB, lower},
		bus,
	)
	require.NoError(t, err)
	cacheA, err := cache.New("users", time.Minute, layeredA)
	require.NoError(t, err)
	cacheB, err := cache.New("users", time.Minute, layeredB)
	require.NoError(t, err)

	listenCtx, cancel := context.WithCancel(context.Background())
	listenerResult := make(chan error, 1)
	cachetest.Go(listenerResult, func() error {
		return layeredB.ListenInvalidations(listenCtx, "users", 1)
	})
	deadline := time.Now().Add(time.Second)
	for bus.SubscriberCount("users") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, 1, bus.SubscriberCount("users"))

	require.NoError(t, cacheA.Set(context.Background(), "alice", "value"))
	value, found, err := cacheB.Get(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value", value)

	require.NoError(t, cacheA.Invalidate(context.Background(), "alice"))
	value, found, err = cacheB.Get(context.Background(), "alice")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, value)

	cancel()
	assert.ErrorIs(t, <-listenerResult, context.Canceled)
}

func TestLayeredInvalidationBusPropagatesNamespaceInvalidation(t *testing.T) {
	bus := memory.NewInvalidationBus[string]()
	lower := memory.New[string, string]()
	upper := memory.New[string, string]()
	layered, err := cache.NewLayeredWithInvalidationBus(
		[]cache.Store[string, string]{upper, lower},
		bus,
	)
	require.NoError(t, err)

	listenCtx, cancel := context.WithCancel(context.Background())
	listenerResult := make(chan error, 1)
	cachetest.Go(listenerResult, func() error {
		return layered.ListenInvalidations(listenCtx, "users", 1)
	})
	deadline := time.Now().Add(time.Second)
	for bus.SubscriberCount("users") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, 1, bus.SubscriberCount("users"))

	entry := cache.Entry[string]{Value: "value", Found: true, ExpiresAt: time.Now().Add(time.Minute)}
	require.NoError(t, upper.Set(context.Background(), "users", "alice", entry))
	require.NoError(t, bus.Publish(context.Background(), cache.InvalidationEvent[string]{
		Namespace: "users",
		Kind:      cache.InvalidateNamespace,
	}))
	_, present, err := upper.Get(context.Background(), "users", "alice")
	require.NoError(t, err)
	assert.False(t, present)

	cancel()
	assert.ErrorIs(t, <-listenerResult, context.Canceled)
}

package cachetest

import (
	"context"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StoreFactory returns a fresh store for each contract subtest.
type StoreFactory func(t *testing.T) cache.Store[string, int]

// RunStoreContract verifies behavior required from every Store implementation.
func RunStoreContract(t *testing.T, factory StoreFactory) {
	t.Helper()

	t.Run("hit and miss", func(t *testing.T) {
		store := factory(t)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))

		got, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.True(t, present)
		assert.Equal(t, entry, got)

		got, present, err = store.Get(context.Background(), "users", "missing")
		require.NoError(t, err)
		assert.False(t, present)
		assert.Equal(t, cache.Entry[int]{}, got)
	})

	t.Run("negative entry", func(t *testing.T) {
		store := factory(t)
		entry := cache.Entry[int]{Found: false, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "missing", entry))

		got, present, err := store.Get(context.Background(), "users", "missing")
		require.NoError(t, err)
		assert.True(t, present)
		assert.False(t, got.Found)
	})

	t.Run("logical expiration", func(t *testing.T) {
		store := factory(t)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(5 * time.Millisecond)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		time.Sleep(20 * time.Millisecond)

		got, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
		assert.Equal(t, cache.Entry[int]{}, got)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		store := factory(t)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))

		_, present, err := store.Get(context.Background(), "orders", "alice")
		require.NoError(t, err)
		assert.False(t, present)
	})

	t.Run("key invalidation", func(t *testing.T) {
		store := factory(t)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		require.NoError(t, store.Invalidate(context.Background(), "users", "alice"))

		_, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
	})

	t.Run("namespace invalidation", func(t *testing.T) {
		store := factory(t)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		require.NoError(t, store.Set(context.Background(), "orders", "alice", entry))
		require.NoError(t, store.InvalidateAll(context.Background(), "users"))

		_, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
		_, present, err = store.Get(context.Background(), "orders", "alice")
		require.NoError(t, err)
		assert.True(t, present)
	})

	t.Run("canceled context", func(t *testing.T) {
		store := factory(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := store.Get(ctx, "users", "alice")
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, store.Set(ctx, "users", "alice", cache.Entry[int]{}), context.Canceled)
		assert.ErrorIs(t, store.Invalidate(ctx, "users", "alice"), context.Canceled)
		assert.ErrorIs(t, store.InvalidateAll(ctx, "users"), context.Canceled)
	})
}

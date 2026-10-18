package memory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FR-CACHE-17
func TestCapacityBoundsNamespaceMetadata(t *testing.T) {
	store, err := NewWithOptions[string, int](Options{MaxEntries: 1, CleanupInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	entry := cache.Entry[int]{Value: 1, Found: true, ExpiresAt: time.Now().Add(time.Hour)}

	for index := range 2_048 {
		require.NoError(t, store.Set(context.Background(), time.Unix(int64(index), 0).String(), "key", entry))
	}

	assert.Equal(t, 1, store.Len())
	assert.LessOrEqual(t, len(store.namespaces), 1)
}

// FR-CACHE-17
func TestInvalidateAllDoesNotRetainEmptyNamespace(t *testing.T) {
	store := New[string, int]()
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	require.NoError(t, store.InvalidateAll(context.Background(), "missing"))
	assert.Empty(t, store.namespaces)
}

// FR-CACHE-20
func TestExpiredLocksAreRemovedDuringChurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		locker := NewLocker[string]()
		for index := range 2_048 {
			_, err := locker.Lock(context.Background(), time.Unix(int64(index), 0).String(), "key", time.Nanosecond)
			require.NoError(t, err)
			time.Sleep(time.Nanosecond)
		}

		_, err := locker.Lock(context.Background(), "live", "key", time.Hour)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(locker.locks), 1)
	})
}

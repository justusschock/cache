package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/cache"
	"github.com/justusschock/cache/backend/memory"
	"github.com/justusschock/cache/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FR-CACHE-06, FR-CACHE-17
func TestStoreContract(t *testing.T) {
	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store := memory.New[string, int]()
		t.Cleanup(func() { assert.NoError(t, store.Close()) })
		return store
	})
}

// FR-CACHE-17
func TestNewWithOptionsRejectsInvalidConfiguration(t *testing.T) {
	for _, options := range []memory.Options{
		{MaxEntries: 0, CleanupInterval: time.Minute},
		{MaxEntries: -1, CleanupInterval: time.Minute},
		{MaxEntries: 1, CleanupInterval: 0},
		{MaxEntries: 1, CleanupInterval: -time.Second},
	} {
		_, err := memory.NewWithOptions[string, int](options)
		assert.ErrorIs(t, err, memory.ErrInvalidOptions)
	}
}

// FR-CACHE-17
func TestStoreEvictsLeastRecentlyUsedEntryAtCapacity(t *testing.T) {
	store, err := memory.NewWithOptions[string, int](memory.Options{
		MaxEntries:      2,
		CleanupInterval: time.Hour,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	entry := func(value int) cache.Entry[int] {
		return cache.Entry[int]{Value: value, Found: true, ExpiresAt: time.Now().Add(time.Hour)}
	}
	assert.NoError(t, store.Set(context.Background(), "users", "alice", entry(1)))
	assert.NoError(t, store.Set(context.Background(), "users", "bob", entry(2)))
	_, _, err = store.Get(context.Background(), "users", "alice")
	assert.NoError(t, err)
	assert.NoError(t, store.Set(context.Background(), "orders", "charlie", entry(3)))

	_, present, err := store.Get(context.Background(), "users", "bob")
	assert.NoError(t, err)
	assert.False(t, present)
	assert.Equal(t, 2, store.Len())
}

// FR-CACHE-05, FR-CACHE-17
func TestBatchStoreContract(t *testing.T) {
	cachetest.RunBatchStoreContract(t, func(t *testing.T) cache.BatchStore[string, int] {
		store := memory.New[string, int]()
		t.Cleanup(func() { assert.NoError(t, store.Close()) })
		return store
	})
}

// FR-CACHE-17
func TestStoreSweepsExpiredEntriesWithoutReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := memory.NewWithOptions[string, int](memory.Options{
			MaxEntries:      10,
			CleanupInterval: time.Minute,
		})
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, store.Close()) })

		entry := cache.Entry[int]{Value: 1, Found: true, ExpiresAt: time.Now().Add(30 * time.Second)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		require.NoError(t, store.Set(context.Background(), "users", "forever", cache.Entry[int]{Value: 2, Found: true}))

		time.Sleep(time.Minute)
		synctest.Wait()
		assert.Equal(t, 1, store.Len())
	})
}

// FR-CACHE-03, FR-CACHE-17
func TestStoreKeepsNoExpiryEntriesAcrossSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := memory.NewWithOptions[string, int](memory.Options{
			MaxEntries:      10,
			CleanupInterval: time.Hour,
		})
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, store.Close()) })

		require.NoError(t, store.SetMany(context.Background(), "users", []string{"alice", "bob"}, []cache.Entry[int]{
			{Value: 1, Found: true},
			{Value: 2, Found: true, ExpiresAt: time.Now().Add(time.Hour)},
		}))

		time.Sleep(365 * 24 * time.Hour)
		synctest.Wait()
		entries, present, err := store.GetMany(context.Background(), "users", []string{"alice", "bob"})
		require.NoError(t, err)
		assert.Equal(t, []bool{true, false}, present)
		assert.Equal(t, 1, entries[0].Value)
		assert.True(t, entries[0].ExpiresAt.IsZero())
		assert.Equal(t, 1, store.Len())
	})
}

// FR-CACHE-17
func TestSetManyRespectsCapacity(t *testing.T) {
	store, err := memory.NewWithOptions[string, int](memory.Options{
		MaxEntries:      2,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	entries := []cache.Entry[int]{{Value: 1, Found: true}, {Value: 2, Found: true}, {Value: 3, Found: true}}
	require.NoError(t, store.SetMany(context.Background(), "users", []string{"a", "b", "c"}, entries))

	_, present, err := store.GetMany(context.Background(), "users", []string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, []bool{false, true, true}, present)
	assert.Equal(t, 2, store.Len())
}

// FR-CACHE-17
func TestStoreCloseIsIdempotent(t *testing.T) {
	store, err := memory.NewWithOptions[string, int](memory.Options{
		MaxEntries:      1,
		CleanupInterval: time.Hour,
	})
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
	assert.NoError(t, store.Close())
}

// NFR-CACHE-01
func TestStoreIsSafeForConcurrentAccess(t *testing.T) {
	store := memory.New[int, int]()
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	const workers = 32
	const operations = 250

	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					errs <- fmt.Errorf("worker %d panicked: %v", worker, recovered)
				}
			}()
			for operation := 0; operation < operations; operation++ {
				key := operation % 16
				entry := cache.Entry[int]{Value: worker, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
				if err := store.Set(context.Background(), "race", key, entry); err != nil {
					errs <- err
					return
				}
				if _, _, err := store.Get(context.Background(), "race", key); err != nil {
					errs <- err
					return
				}
				if operation%17 == 0 {
					if err := store.Invalidate(context.Background(), "race", key); err != nil {
						errs <- err
						return
					}
				}
				if operation%101 == 0 {
					if err := store.InvalidateAll(context.Background(), "race"); err != nil {
						errs <- err
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
}

package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
)

func TestStoreContract(t *testing.T) {
	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store := memory.New[string, int]()
		t.Cleanup(func() { assert.NoError(t, store.Close()) })
		return store
	})
}

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

func TestStoreSweepsExpiredEntriesWithoutReads(t *testing.T) {
	store, err := memory.NewWithOptions[string, int](memory.Options{
		MaxEntries:      10,
		CleanupInterval: time.Millisecond,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	entry := cache.Entry[int]{Value: 1, Found: true, ExpiresAt: time.Now().Add(5 * time.Millisecond)}
	assert.NoError(t, store.Set(context.Background(), "users", "alice", entry))
	assert.Eventually(t, func() bool { return store.Len() == 0 }, time.Second, time.Millisecond)
}

func TestStoreCloseIsIdempotent(t *testing.T) {
	store, err := memory.NewWithOptions[string, int](memory.Options{
		MaxEntries:      1,
		CleanupInterval: time.Hour,
	})
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
	assert.NoError(t, store.Close())
}

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

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
	cachetest.RunStoreContract(t, func(*testing.T) cache.Store[string, int] {
		return memory.New[string, int]()
	})
}

func TestStoreIsSafeForConcurrentAccess(t *testing.T) {
	store := memory.New[int, int]()
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

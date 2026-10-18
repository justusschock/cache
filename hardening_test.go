package cache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FR-CACHE-09
func TestCanceledFlightWaitersDoNotCancelOrDuplicateOwningLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		pull, err := cache.NewPullThrough("users", base, func(context.Context, string) (string, bool, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return "value", true, nil
		})
		require.NoError(t, err)

		owner := make(chan error, 1)
		go func() {
			_, _, getErr := pull.Get(context.Background(), "alice")
			owner <- getErr
		}()
		<-started

		const waiters = 100
		results := make(chan error, waiters)
		cancels := make([]context.CancelFunc, 0, waiters)
		for range waiters {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)
			go func() {
				_, _, getErr := pull.Get(ctx, "alice")
				results <- getErr
			}()
		}
		for _, cancel := range cancels {
			cancel()
		}
		for range waiters {
			assert.ErrorIs(t, <-results, context.Canceled)
		}

		close(release)
		assert.NoError(t, <-owner)
		assert.Equal(t, int32(1), calls.Load())
	})
}

// FR-CACHE-10
func TestFlightOwnerErrorRemainsRetryableAfterCanceledWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newMemoryCache(t)
		wantErr := errors.New("source unavailable")
		var calls atomic.Int32
		pull, err := cache.NewPullThrough("users", base, func(context.Context, string) (string, bool, error) {
			calls.Add(1)
			return "", false, wantErr
		})
		require.NoError(t, err)

		for range 2 {
			_, _, getErr := pull.Get(context.Background(), "alice")
			assert.ErrorIs(t, getErr, wantErr)
		}
		assert.Equal(t, int32(2), calls.Load())
	})
}

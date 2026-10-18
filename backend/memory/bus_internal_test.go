package memory

import (
	"context"
	"testing"
	"testing/synctest"

	cache "github.com/justusschock/cache"
	"github.com/stretchr/testify/assert"
)

// FR-CACHE-15
func TestInvalidationBusRemovesSubscriberWhenContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewInvalidationBus[string]()
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			result <- bus.Subscribe(ctx, "users", func(context.Context, cache.InvalidationEvent[string]) error {
				return nil
			})
		}()
		synctest.Wait()
		assert.Equal(t, 1, bus.SubscriberCount("users"))

		cancel()
		assert.ErrorIs(t, <-result, context.Canceled)
		assert.Zero(t, bus.SubscriberCount("users"))
		assert.Empty(t, bus.subscribers)
	})
}

package cachetest

import (
	"context"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunInvalidationBusContract verifies cross-instance delivery and cancellation.
func RunInvalidationBusContract[K comparable](
	t *testing.T,
	newBus func(*testing.T) cache.InvalidationBus[K],
	event cache.InvalidationEvent[K],
) {
	t.Helper()
	subscriber := newBus(t)
	publisher := newBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	listenerResult := make(chan error, 1)
	received := make(chan cache.InvalidationEvent[K], 1)
	Go(listenerResult, func() error {
		return subscriber.Subscribe(ctx, event.Namespace, func(_ context.Context, delivered cache.InvalidationEvent[K]) error {
			select {
			case received <- delivered:
			default:
			}
			return nil
		})
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		require.NoError(t, publisher.Publish(context.Background(), event))
		select {
		case delivered := <-received:
			assert.Equal(t, event, delivered)
			cancel()
			assert.ErrorIs(t, <-listenerResult, context.Canceled)
			return
		case <-time.After(20 * time.Millisecond):
			if time.Now().After(deadline) {
				cancel()
				require.Fail(t, "invalidation event was not delivered")
			}
		}
	}
}

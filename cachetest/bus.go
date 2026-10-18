package cachetest

import (
	"context"
	"testing"
	"time"

	cache "github.com/justusschock/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunInvalidationBusContract verifies the behaviour required from every
// cache.InvalidationBus: Subscribe rejects a nil handler with
// cache.ErrNilInvalidationHandler, an event published through one bus
// instance reaches a subscriber on another instance unchanged, a repeated
// publication is delivered again, and Subscribe returns context.Canceled once
// its context is canceled. newBus is called twice, once for the subscriber
// and once for the publisher; both instances must share a transport.
func RunInvalidationBusContract[K comparable](
	t *testing.T,
	newBus func(*testing.T) cache.InvalidationBus[K],
	event cache.InvalidationEvent[K],
) {
	t.Helper()
	subscriber := newBus(t)
	publisher := newBus(t)
	nilHandlerCtx, cancelNilHandler := context.WithCancel(context.Background())
	cancelNilHandler()
	assert.ErrorIs(
		t,
		subscriber.Subscribe(nilHandlerCtx, event.Namespace, nil),
		cache.ErrNilInvalidationHandler,
	)
	ctx, cancel := context.WithCancel(context.Background())
	listenerResult := make(chan error, 1)
	received := make(chan cache.InvalidationEvent[K], 2)
	goReport(listenerResult, func() error {
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
			require.NoError(t, publisher.Publish(context.Background(), event))
			select {
			case duplicate := <-received:
				assert.Equal(t, event, duplicate)
				cancel()
				assert.ErrorIs(t, <-listenerResult, context.Canceled)
				return
			case <-time.After(time.Second):
				cancel()
				require.Fail(t, "duplicate invalidation event was not delivered")
			}
		case <-time.After(20 * time.Millisecond):
			if time.Now().After(deadline) {
				cancel()
				require.Fail(t, "invalidation event was not delivered")
			}
		}
	}
}

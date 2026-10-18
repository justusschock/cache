package cache_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-messaging"
	messagingmemory "github.com/justusschock/go-messaging/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type messagingPublisher[K comparable] struct {
	envelopes []messaging.Envelope[cache.InvalidationEvent[K]]
	err       error
}

func (p *messagingPublisher[K]) Publish(
	_ context.Context,
	envelope messaging.Envelope[cache.InvalidationEvent[K]],
) error {
	p.envelopes = append(p.envelopes, envelope)
	return p.err
}

type messagingSubscriber[K comparable] struct {
	delivery     messaging.Delivery[cache.InvalidationEvent[K]]
	subscription messaging.Subscription
}

func (s *messagingSubscriber[K]) Subscribe(
	ctx context.Context,
	subscription messaging.Subscription,
	handler messaging.Handler[cache.InvalidationEvent[K]],
) error {
	s.subscription = subscription
	if s.delivery == nil {
		return nil
	}
	return handler(ctx, s.delivery)
}

type messagingDelivery[K comparable] struct {
	envelope messaging.Envelope[cache.InvalidationEvent[K]]
	acks     int
	nacks    int
	nackErr  error
}

func (d *messagingDelivery[K]) Envelope() messaging.Envelope[cache.InvalidationEvent[K]] {
	return d.envelope
}

func (*messagingDelivery[K]) Attempt() uint { return 1 }

func (d *messagingDelivery[K]) Ack(context.Context) error {
	d.acks++
	return nil
}

func (d *messagingDelivery[K]) Nack(_ context.Context, err error) error {
	d.nacks++
	d.nackErr = err
	return nil
}

func invalidationDelivery(t *testing.T, event cache.InvalidationEvent[string]) *messagingDelivery[string] {
	t.Helper()
	envelope, err := messaging.NewEnvelope("cache.invalidations."+event.Namespace, event)
	require.NoError(t, err)
	return &messagingDelivery[string]{envelope: envelope}
}

func subscriptionFor(namespace string) messaging.Subscription {
	return messaging.Subscription{
		Subject:     "cache.invalidations." + namespace,
		Consumer:    "cache-test",
		Semantics:   messaging.AtLeastOnce,
		MaxInFlight: 1,
	}
}

// FR-CACHE-22
func TestNewMessagingInvalidationBusRejectsNilDependencies(t *testing.T) {
	publisher := &messagingPublisher[string]{}
	subscriber := &messagingSubscriber[string]{}

	_, err := cache.NewMessagingInvalidationBus[string](nil, subscriber, subscriptionFor)
	assert.ErrorIs(t, err, cache.ErrNilMessagingPublisher)
	_, err = cache.NewMessagingInvalidationBus[string](publisher, nil, subscriptionFor)
	assert.ErrorIs(t, err, cache.ErrNilMessagingSubscriber)
	_, err = cache.NewMessagingInvalidationBus[string](publisher, subscriber, nil)
	assert.ErrorIs(t, err, cache.ErrNilSubscriptionFactory)
}

// FR-CACHE-16
func TestMessagingBusRejectsNilHandler(t *testing.T) {
	bus, err := cache.NewMessagingInvalidationBus(
		&messagingPublisher[string]{},
		&messagingSubscriber[string]{},
		subscriptionFor,
	)
	require.NoError(t, err)

	err = bus.Subscribe(context.Background(), "users", nil)
	assert.ErrorIs(t, err, cache.ErrNilInvalidationHandler)
}

// FR-CACHE-16
func TestMessagingBusPublishesTypedEnvelope(t *testing.T) {
	publisher := &messagingPublisher[string]{}
	bus, err := cache.NewMessagingInvalidationBus(
		publisher,
		&messagingSubscriber[string]{},
		subscriptionFor,
	)
	require.NoError(t, err)
	event := cache.InvalidationEvent[string]{
		Namespace: "users", Kind: cache.InvalidateKey, Key: "alice", Generation: 7,
	}

	require.NoError(t, bus.Publish(context.Background(), event))
	require.Len(t, publisher.envelopes, 1)
	assert.Equal(t, "cache.invalidations.users", publisher.envelopes[0].Subject)
	assert.Equal(t, event, publisher.envelopes[0].Payload)
}

// FR-CACHE-16
func TestMessagingBusPublishesToFactorySubject(t *testing.T) {
	publisher := &messagingPublisher[string]{}
	factory := func(namespace string) messaging.Subscription {
		return messaging.Subscription{
			Subject: "tenant-cache." + namespace, Consumer: "cache-test",
			Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
		}
	}
	bus, err := cache.NewMessagingInvalidationBus(publisher, &messagingSubscriber[string]{}, factory)
	require.NoError(t, err)
	event := cache.InvalidationEvent[string]{Namespace: "users", Kind: cache.InvalidateNamespace}

	require.NoError(t, bus.Publish(context.Background(), event))
	require.Len(t, publisher.envelopes, 1)
	assert.Equal(t, factory("users").Subject, publisher.envelopes[0].Subject)
}

// FR-CACHE-16
func TestMessagingBusAcknowledgesHandledEvent(t *testing.T) {
	delivery := invalidationDelivery(t, cache.InvalidationEvent[string]{
		Namespace: "users", Kind: cache.InvalidateKey, Key: "alice",
	})
	subscriber := &messagingSubscriber[string]{delivery: delivery}
	bus, err := cache.NewMessagingInvalidationBus(
		&messagingPublisher[string]{},
		subscriber,
		subscriptionFor,
	)
	require.NoError(t, err)

	require.NoError(t, bus.Subscribe(
		context.Background(),
		"users",
		func(context.Context, cache.InvalidationEvent[string]) error { return nil },
	))
	assert.Equal(t, 1, delivery.acks)
	assert.Zero(t, delivery.nacks)
	assert.Equal(t, subscriptionFor("users"), subscriber.subscription)
}

// FR-CACHE-16
func TestMessagingBusNacksHandlerError(t *testing.T) {
	delivery := invalidationDelivery(t, cache.InvalidationEvent[string]{
		Namespace: "users", Kind: cache.InvalidateKey, Key: "alice",
	})
	bus, err := cache.NewMessagingInvalidationBus(
		&messagingPublisher[string]{},
		&messagingSubscriber[string]{delivery: delivery},
		subscriptionFor,
	)
	require.NoError(t, err)
	wantErr := errors.New("eviction failed")

	err = bus.Subscribe(
		context.Background(),
		"users",
		func(context.Context, cache.InvalidationEvent[string]) error { return wantErr },
	)
	assert.ErrorIs(t, err, wantErr)
	assert.Zero(t, delivery.acks)
	assert.Equal(t, 1, delivery.nacks)
	assert.ErrorIs(t, delivery.nackErr, wantErr)
}

// FR-CACHE-16
func TestMessagingBusAcknowledgesMismatchedNamespaceWithoutHandling(t *testing.T) {
	delivery := invalidationDelivery(t, cache.InvalidationEvent[string]{
		Namespace: "orders", Kind: cache.InvalidateNamespace,
	})
	bus, err := cache.NewMessagingInvalidationBus(
		&messagingPublisher[string]{},
		&messagingSubscriber[string]{delivery: delivery},
		subscriptionFor,
	)
	require.NoError(t, err)
	called := false

	require.NoError(t, bus.Subscribe(
		context.Background(),
		"users",
		func(context.Context, cache.InvalidationEvent[string]) error {
			called = true
			return nil
		},
	))
	assert.False(t, called)
	assert.Equal(t, 1, delivery.acks)
	assert.Zero(t, delivery.nacks)
}

// FR-CACHE-16
func TestMessagingBusRoundTripsThroughMemoryBroker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := messagingmemory.NewBroker[cache.InvalidationEvent[string]](time.Millisecond)
		t.Cleanup(func() { assert.NoError(t, broker.Close()) })
		bus, err := cache.NewMessagingInvalidationBus[string](broker, broker, subscriptionFor)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		received := make(chan cache.InvalidationEvent[string], 1)
		result := make(chan error, 1)
		go func() {
			result <- bus.Subscribe(ctx, "users", func(_ context.Context, event cache.InvalidationEvent[string]) error {
				select {
				case received <- event:
				default:
				}
				return nil
			})
		}()

		event := cache.InvalidationEvent[string]{Namespace: "users", Kind: cache.InvalidateKey, Key: "alice"}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			require.NoError(t, bus.Publish(ctx, event))
			select {
			case got := <-received:
				assert.Equal(t, event, got)
				cancel()
				assert.ErrorIs(t, <-result, context.Canceled)
				return
			case <-ticker.C:
			case <-ctx.Done():
				require.NoError(t, context.Cause(ctx))
			}
		}
	})
}

var _ cache.InvalidationBus[string] = (*cache.MessagingInvalidationBus[string])(nil)

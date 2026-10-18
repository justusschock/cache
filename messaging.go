package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/justusschock/messaging"
)

// Errors returned by NewMessagingInvalidationBus.
var (
	ErrNilMessagingPublisher  = errors.New("cache: messaging publisher must not be nil")
	ErrNilMessagingSubscriber = errors.New("cache: messaging subscriber must not be nil")
	ErrNilSubscriptionFactory = errors.New("cache: subscription factory must not be nil")
)

// SubscriptionFactory maps a namespace to the deterministic subscription used
// for both publishing and subscribing. In particular, Subject must be stable.
type SubscriptionFactory func(namespace string) messaging.Subscription

// MessagingInvalidationBus is an InvalidationBus over typed messaging
// publishers and subscribers. It is safe for concurrent use if they are.
type MessagingInvalidationBus[K comparable] struct {
	publisher  messaging.Publisher[InvalidationEvent[K]]
	subscriber messaging.Subscriber[InvalidationEvent[K]]
	factory    SubscriptionFactory
}

// NewMessagingInvalidationBus constructs a bus that publishes to and
// subscribes on the subscription that factory returns for each namespace.
func NewMessagingInvalidationBus[K comparable](
	publisher messaging.Publisher[InvalidationEvent[K]],
	subscriber messaging.Subscriber[InvalidationEvent[K]],
	factory SubscriptionFactory,
) (*MessagingInvalidationBus[K], error) {
	if isNil(publisher) {
		return nil, ErrNilMessagingPublisher
	}
	if isNil(subscriber) {
		return nil, ErrNilMessagingSubscriber
	}
	if factory == nil {
		return nil, ErrNilSubscriptionFactory
	}
	return &MessagingInvalidationBus[K]{
		publisher:  publisher,
		subscriber: subscriber,
		factory:    factory,
	}, nil
}

// Publish wraps event in an envelope addressed to the subject of its
// namespace's subscription and publishes it.
func (b *MessagingInvalidationBus[K]) Publish(ctx context.Context, event InvalidationEvent[K]) error {
	envelope, err := messaging.NewEnvelope(b.factory(event.Namespace).Subject, event)
	if err != nil {
		return fmt.Errorf("cache messaging envelope: %w", err)
	}
	if err := b.publisher.Publish(ctx, envelope); err != nil {
		return fmt.Errorf("cache messaging publish: %w", err)
	}
	return nil
}

// Subscribe follows the messaging subscriber lifetime and normally blocks
// until ctx ends. Handled deliveries and deliveries for another namespace are
// acknowledged; handler failures are negatively acknowledged so that an
// at-least-once transport redelivers them.
func (b *MessagingInvalidationBus[K]) Subscribe(
	ctx context.Context,
	namespace string,
	handler InvalidationHandler[K],
) error {
	if handler == nil {
		return ErrNilInvalidationHandler
	}
	err := b.subscriber.Subscribe(ctx, b.factory(namespace), func(
		deliveryCtx context.Context,
		delivery messaging.Delivery[InvalidationEvent[K]],
	) error {
		event := delivery.Envelope().Payload
		if event.Namespace != namespace {
			return delivery.Ack(deliveryCtx)
		}
		if err := handler(deliveryCtx, event); err != nil {
			return errors.Join(err, delivery.Nack(deliveryCtx, err))
		}
		return delivery.Ack(deliveryCtx)
	})
	if err != nil {
		return fmt.Errorf("cache messaging subscribe: %w", err)
	}
	return nil
}

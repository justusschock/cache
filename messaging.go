package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/justusschock/go-messaging"
)

var (
	ErrNilMessagingPublisher  = errors.New("cache: messaging publisher must not be nil")
	ErrNilMessagingSubscriber = errors.New("cache: messaging subscriber must not be nil")
	ErrNilSubscriptionFactory = errors.New("cache: subscription factory must not be nil")
	ErrNilInvalidationHandler = errors.New("cache: invalidation handler must not be nil")
)

type SubscriptionFactory func(namespace string) messaging.Subscription

type MessagingInvalidationBus[K comparable] struct {
	publisher  messaging.Publisher[InvalidationEvent[K]]
	subscriber messaging.Subscriber[InvalidationEvent[K]]
	factory    SubscriptionFactory
}

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

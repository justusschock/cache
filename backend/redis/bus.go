package redis

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	goredis "github.com/redis/go-redis/v9"
)

type invalidationConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// InvalidationOption configures Redis invalidation serialization.
type InvalidationOption[K comparable] func(*invalidationConfig[K]) error

// WithInvalidationKeyCodec overrides automatic invalidation-key serialization.
func WithInvalidationKeyCodec[K comparable](codec cache.Codec[K]) InvalidationOption[K] {
	return func(config *invalidationConfig[K]) error {
		if codec == nil {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// InvalidationBus distributes invalidations through Redis Pub/Sub.
type InvalidationBus[K comparable] struct {
	client   goredis.UniversalClient
	keyCodec cache.Codec[K]
}

// NewInvalidationBus constructs a Redis Pub/Sub invalidation bus.
func NewInvalidationBus[K comparable](client goredis.UniversalClient, options ...InvalidationOption[K]) (*InvalidationBus[K], error) {
	config := invalidationConfig[K]{keyCodec: cache.DefaultCodec[K]()}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	if client == nil {
		return nil, ErrNilClient
	}
	return &InvalidationBus[K]{client: client, keyCodec: config.keyCodec}, nil
}

func (b *InvalidationBus[K]) Publish(ctx context.Context, event cache.InvalidationEvent[K]) error {
	payload, err := wire.EncodeInvalidation(event, b.keyCodec)
	if err != nil {
		return err
	}
	if err := b.client.Publish(ctx, invalidationChannel(event.Namespace), payload).Err(); err != nil {
		return fmt.Errorf("cache redis publish invalidation: %w", err)
	}
	return nil
}

func (b *InvalidationBus[K]) Subscribe(ctx context.Context, namespace string, handler cache.InvalidationHandler[K]) error {
	pubsub := b.client.Subscribe(ctx, invalidationChannel(namespace))
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("cache redis subscribe invalidation: %w", err)
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-messages:
			if !ok {
				return errors.New("cache redis: invalidation subscription closed")
			}
			event, err := wire.DecodeInvalidation([]byte(message.Payload), b.keyCodec)
			if err != nil {
				return err
			}
			if err := handler(ctx, event); err != nil {
				return err
			}
		}
	}
}

func invalidationChannel(namespace string) string {
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:i:" + hex.EncodeToString(namespaceHash[:])
}

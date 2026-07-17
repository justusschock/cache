package hazelcast

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	hazelcastclient "github.com/hazelcast/hazelcast-go-client"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
)

type invalidationConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// InvalidationOption configures Hazelcast invalidation serialization.
type InvalidationOption[K comparable] func(*invalidationConfig[K]) error

// WithInvalidationKeyCodec overrides automatic invalidation-key serialization.
func WithInvalidationKeyCodec[K comparable](codec cache.Codec[K]) InvalidationOption[K] {
	return func(config *invalidationConfig[K]) error {
		if isNil(codec) {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// InvalidationBus distributes invalidations through a Hazelcast topic.
type InvalidationBus[K comparable] struct {
	topic    *hazelcastclient.Topic
	keyCodec cache.Codec[K]
}

// NewInvalidationBus obtains the shared invalidation topic from a caller-owned client.
func NewInvalidationBus[K comparable](ctx context.Context, client *hazelcastclient.Client, options ...InvalidationOption[K]) (*InvalidationBus[K], error) {
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
	topic, err := client.GetTopic(ctx, "go-cache-invalidation")
	if err != nil {
		return nil, fmt.Errorf("cache hazelcast get invalidation topic: %w", err)
	}
	return &InvalidationBus[K]{topic: topic, keyCodec: config.keyCodec}, nil
}

func (b *InvalidationBus[K]) Publish(ctx context.Context, event cache.InvalidationEvent[K]) error {
	payload, err := wire.EncodeInvalidation(event, b.keyCodec)
	if err != nil {
		return err
	}
	if err := b.topic.Publish(ctx, payload); err != nil {
		return fmt.Errorf("cache hazelcast publish invalidation: %w", err)
	}
	return nil
}

func (b *InvalidationBus[K]) Subscribe(ctx context.Context, namespace string, handler cache.InvalidationHandler[K]) error {
	if handler == nil {
		return cache.ErrNilInvalidationHandler
	}
	callbackErrors := make(chan error, 1)
	var once sync.Once
	report := func(err error) {
		once.Do(func() {
			callbackErrors <- err
		})
	}
	subscriptionID, err := b.topic.AddMessageListener(ctx, func(message *hazelcastclient.MessagePublished) {
		defer func() {
			if recovered := recover(); recovered != nil {
				report(fmt.Errorf("cache hazelcast invalidation handler panic: %v", recovered))
			}
		}()
		var payload []byte
		switch value := message.Value.(type) {
		case []byte:
			payload = value
		case string:
			payload = []byte(value)
		default:
			report(fmt.Errorf("cache hazelcast: unexpected invalidation type %T", message.Value))
			return
		}
		event, decodeErr := wire.DecodeInvalidation(payload, b.keyCodec)
		if decodeErr != nil {
			report(decodeErr)
			return
		}
		if event.Namespace != namespace {
			return
		}
		if handleErr := handler(ctx, event); handleErr != nil {
			report(handleErr)
		}
	})
	if err != nil {
		return fmt.Errorf("cache hazelcast subscribe invalidation: %w", err)
	}

	var result error
	select {
	case <-ctx.Done():
		result = ctx.Err()
	case result = <-callbackErrors:
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := b.topic.RemoveListener(cleanupCtx, subscriptionID); err != nil {
		result = errors.Join(result, fmt.Errorf("cache hazelcast remove invalidation listener: %w", err))
	}
	return result
}

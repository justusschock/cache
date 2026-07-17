package valkey

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	valkeyclient "github.com/valkey-io/valkey-go"
)

type invalidationConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// InvalidationOption configures Valkey invalidation serialization.
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

// InvalidationBus distributes invalidations through Valkey Pub/Sub.
type InvalidationBus[K comparable] struct {
	client   valkeyclient.Client
	keyCodec cache.Codec[K]
}

// NewInvalidationBus constructs a Valkey Pub/Sub invalidation bus.
func NewInvalidationBus[K comparable](client valkeyclient.Client, options ...InvalidationOption[K]) (*InvalidationBus[K], error) {
	config := invalidationConfig[K]{keyCodec: cache.DefaultCodec[K]()}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	if isNil(client) {
		return nil, ErrNilClient
	}
	return &InvalidationBus[K]{client: client, keyCodec: config.keyCodec}, nil
}

func (b *InvalidationBus[K]) Publish(ctx context.Context, event cache.InvalidationEvent[K]) error {
	payload, err := wire.EncodeInvalidation(event, b.keyCodec)
	if err != nil {
		return err
	}
	command := b.client.B().Publish().Channel(invalidationChannel(event.Namespace)).Message(string(payload)).Build()
	if err := b.client.Do(ctx, command).Error(); err != nil {
		return fmt.Errorf("cache valkey publish invalidation: %w", err)
	}
	return nil
}

func (b *InvalidationBus[K]) Subscribe(ctx context.Context, namespace string, handler cache.InvalidationHandler[K]) error {
	subscriptionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	var handlerErr error
	setHandlerError := func(err error) {
		mu.Lock()
		if handlerErr == nil {
			handlerErr = err
			cancel()
		}
		mu.Unlock()
	}
	command := b.client.B().Subscribe().Channel(invalidationChannel(namespace)).Build()
	err := b.client.Receive(subscriptionCtx, command, func(message valkeyclient.PubSubMessage) {
		defer func() {
			if recovered := recover(); recovered != nil {
				setHandlerError(fmt.Errorf("cache valkey invalidation handler panic: %v", recovered))
			}
		}()
		event, decodeErr := wire.DecodeInvalidation([]byte(message.Message), b.keyCodec)
		if decodeErr != nil {
			setHandlerError(decodeErr)
			return
		}
		if event.Namespace != namespace {
			return
		}
		if handleErr := handler(subscriptionCtx, event); handleErr != nil {
			setHandlerError(handleErr)
		}
	})
	mu.Lock()
	callbackErr := handlerErr
	mu.Unlock()
	if callbackErr != nil {
		return callbackErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("cache valkey subscribe invalidation: %w", err)
	}
	return nil
}

func invalidationChannel(namespace string) string {
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:i:" + hex.EncodeToString(namespaceHash[:])
}

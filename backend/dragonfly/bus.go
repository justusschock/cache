package dragonfly

import (
	cache "github.com/justusschock/go-cache"
	rediscache "github.com/justusschock/go-cache/backend/redis"
	goredis "github.com/redis/go-redis/v9"
)

type invalidationConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// InvalidationOption configures Dragonfly invalidation serialization.
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

// InvalidationBus distributes events through Dragonfly's Redis-compatible Pub/Sub.
type InvalidationBus[K comparable] struct {
	*rediscache.InvalidationBus[K]
}

// NewInvalidationBus constructs a Dragonfly invalidation bus.
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
	bus, err := rediscache.NewInvalidationBus(client, rediscache.WithInvalidationKeyCodec(config.keyCodec))
	if err != nil {
		return nil, err
	}
	return &InvalidationBus[K]{InvalidationBus: bus}, nil
}

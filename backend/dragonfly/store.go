// Package dragonfly provides a Dragonfly-backed cache store through its Redis protocol.
package dragonfly

import (
	cache "github.com/justusschock/cache"
	rediscache "github.com/justusschock/cache/backend/redis"
	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrNilClient = rediscache.ErrNilClient
	ErrNilCodec  = rediscache.ErrNilCodec
)

type config[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures Dragonfly serialization.
type Option[K comparable, V any] func(*config[K, V]) error

// WithKeyCodec overrides automatic key serialization.
func WithKeyCodec[K comparable, V any](codec cache.Codec[K]) Option[K, V] {
	return func(config *config[K, V]) error {
		if isNil(codec) {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// WithValueCodec overrides automatic value serialization.
func WithValueCodec[K comparable, V any](codec cache.Codec[V]) Option[K, V] {
	return func(config *config[K, V]) error {
		if isNil(codec) {
			return ErrNilCodec
		}
		config.valueCodec = codec
		return nil
	}
}

// Store is a Dragonfly cache store using Redis-compatible commands.
type Store[K comparable, V any] struct {
	*rediscache.Store[K, V]
}

// New constructs a Dragonfly store using a caller-owned Redis client.
func New[K comparable, V any](client goredis.UniversalClient, options ...Option[K, V]) (*Store[K, V], error) {
	config := config[K, V]{keyCodec: cache.DefaultCodec[K](), valueCodec: cache.DefaultCodec[V]()}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	store, err := rediscache.New(
		client,
		rediscache.WithKeyCodec[K, V](config.keyCodec),
		rediscache.WithValueCodec[K, V](config.valueCodec),
	)
	if err != nil {
		return nil, err
	}
	return &Store[K, V]{Store: store}, nil
}

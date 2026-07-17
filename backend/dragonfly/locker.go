package dragonfly

import (
	cache "github.com/justusschock/go-cache"
	rediscache "github.com/justusschock/go-cache/backend/redis"
	goredis "github.com/redis/go-redis/v9"
)

type lockerConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// LockerOption configures Dragonfly lock key serialization.
type LockerOption[K comparable] func(*lockerConfig[K]) error

// WithLockerKeyCodec overrides automatic lock key serialization.
func WithLockerKeyCodec[K comparable](codec cache.Codec[K]) LockerOption[K] {
	return func(config *lockerConfig[K]) error {
		if isNil(codec) {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// Locker provides renewable Dragonfly leases through Redis-compatible scripts.
type Locker[K comparable] struct {
	*rediscache.Locker[K]
}

// NewLocker constructs a Dragonfly locker using a caller-owned Redis client.
func NewLocker[K comparable](client goredis.UniversalClient, options ...LockerOption[K]) (*Locker[K], error) {
	config := lockerConfig[K]{keyCodec: cache.DefaultCodec[K]()}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	locker, err := rediscache.NewLocker(client, rediscache.WithLockerKeyCodec(config.keyCodec))
	if err != nil {
		return nil, err
	}
	return &Locker[K]{Locker: locker}, nil
}

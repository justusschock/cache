// Package redis provides a Redis-backed cache store.
package redis

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrNilClient = errors.New("cache redis: client must not be nil")
	ErrNilCodec  = errors.New("cache redis: codec must not be nil")
)

type config[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures Redis serialization.
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

// Store persists entries in Redis. The caller retains ownership of the client.
type Store[K comparable, V any] struct {
	client  goredis.UniversalClient
	encoder *wire.Encoder[K, V]
}

// New constructs a Redis store using a caller-owned client.
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
	if isNil(client) {
		return nil, ErrNilClient
	}
	return &Store[K, V]{client: client, encoder: wire.New(config.keyCodec, config.valueCodec)}, nil
}

func (s *Store[K, V]) Get(ctx context.Context, namespace string, key K) (cache.Entry[V], bool, error) {
	var zero cache.Entry[V]
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	entryKey, generationKey, err := s.keys(namespace, key)
	if err != nil {
		return zero, false, err
	}
	envelope, err := s.client.Get(ctx, entryKey).Bytes()
	if errors.Is(err, goredis.Nil) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("cache redis get: %w", err)
	}
	generation, err := s.generation(ctx, generationKey)
	if err != nil {
		return zero, false, err
	}
	entry, recordGeneration, err := s.encoder.Decode(envelope)
	if err != nil {
		return zero, false, fmt.Errorf("cache redis decode: %w", err)
	}
	if recordGeneration != generation || !entry.ExpiresAt.After(time.Now()) {
		return zero, false, nil
	}
	return entry, true, nil
}

func (s *Store[K, V]) Set(ctx context.Context, namespace string, key K, entry cache.Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryKey, generationKey, err := s.keys(namespace, key)
	if err != nil {
		return err
	}
	ttl := time.Until(entry.ExpiresAt)
	if ttl <= 0 {
		return s.Invalidate(ctx, namespace, key)
	}
	generation, err := s.generation(ctx, generationKey)
	if err != nil {
		return err
	}
	envelope, err := s.encoder.Encode(entry, generation)
	if err != nil {
		return err
	}
	if err := s.client.Set(ctx, entryKey, envelope, ttl).Err(); err != nil {
		return fmt.Errorf("cache redis set: %w", err)
	}
	return nil
}

func (s *Store[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryKey, _, err := s.keys(namespace, key)
	if err != nil {
		return err
	}
	if err := s.client.Del(ctx, entryKey).Err(); err != nil {
		return fmt.Errorf("cache redis invalidate: %w", err)
	}
	return nil
}

func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	generationKey := redisGenerationKey(namespace)
	if err := s.client.Incr(ctx, generationKey).Err(); err != nil {
		return fmt.Errorf("cache redis invalidate all: %w", err)
	}
	return nil
}

func (s *Store[K, V]) keys(namespace string, key K) (string, string, error) {
	physicalKey, err := s.encoder.Key(key)
	if err != nil {
		return "", "", err
	}
	namespaceHash := wire.Namespace(namespace)
	namespacePart := hex.EncodeToString(namespaceHash[:])
	return "go-cache:e:" + namespacePart + ":" + hex.EncodeToString(physicalKey[:]), "go-cache:g:" + namespacePart, nil
}

func redisGenerationKey(namespace string) string {
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:g:" + hex.EncodeToString(namespaceHash[:])
}

func (s *Store[K, V]) generation(ctx context.Context, key string) (uint64, error) {
	generation, err := s.client.Get(ctx, key).Uint64()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cache redis generation: %w", err)
	}
	return generation, nil
}

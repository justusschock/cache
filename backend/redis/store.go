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

// Get returns the live entry stored under key in namespace and whether one
// is present. Expired entries and entries written before the namespace's
// last InvalidateAll are reported as absent.
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
	if recordGeneration != generation || entry.Expired(time.Now()) {
		return zero, false, nil
	}
	return entry, true, nil
}

// Set stores entry under key in namespace, replacing any previous entry.
// An entry with a zero ExpiresAt never expires.
func (s *Store[K, V]) Set(ctx context.Context, namespace string, key K, entry cache.Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryKey, generationKey, err := s.keys(namespace, key)
	if err != nil {
		return err
	}
	ttl, live := redisTTL(entry)
	if !live {
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

// Invalidate removes key from namespace. Removing a missing key is not an
// error.
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

// InvalidateAll hides every entry in namespace by advancing the namespace
// generation. Older records are ignored on read and left to expire or be
// overwritten.
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

// GetMany reads keys and the namespace generation in one pipeline round trip.
// Results are aligned with keys.
func (s *Store[K, V]) GetMany(ctx context.Context, namespace string, keys []K) ([]cache.Entry[V], []bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	entries := make([]cache.Entry[V], len(keys))
	present := make([]bool, len(keys))
	if len(keys) == 0 {
		return entries, present, nil
	}
	entryKeys, err := s.entryKeys(namespace, keys)
	if err != nil {
		return nil, nil, err
	}
	pipeline := s.client.Pipeline()
	commands := make([]*goredis.StringCmd, len(entryKeys))
	for index, entryKey := range entryKeys {
		commands[index] = pipeline.Get(ctx, entryKey)
	}
	generationCommand := pipeline.Get(ctx, redisGenerationKey(namespace))
	if _, err := pipeline.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return nil, nil, fmt.Errorf("cache redis get many: %w", err)
	}
	generation, err := generationCommand.Uint64()
	if errors.Is(err, goredis.Nil) {
		generation, err = 0, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("cache redis generation: %w", err)
	}
	now := time.Now()
	for index, command := range commands {
		envelope, err := command.Bytes()
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("cache redis get many: %w", err)
		}
		entry, recordGeneration, err := s.encoder.Decode(envelope)
		if err != nil {
			return nil, nil, fmt.Errorf("cache redis decode: %w", err)
		}
		if recordGeneration != generation || entry.Expired(now) {
			continue
		}
		entries[index], present[index] = entry, true
	}
	return entries, present, nil
}

// SetMany writes entries in one pipeline round trip after reading the
// namespace generation. Already expired entries are deleted instead. The
// pipeline is not a transaction, so a failed call may leave part of the batch
// applied.
func (s *Store[K, V]) SetMany(ctx context.Context, namespace string, keys []K, entries []cache.Entry[V]) error {
	if len(keys) != len(entries) {
		return cache.ErrBatchLength
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	entryKeys, err := s.entryKeys(namespace, keys)
	if err != nil {
		return err
	}
	generation, err := s.generation(ctx, redisGenerationKey(namespace))
	if err != nil {
		return err
	}
	pipeline := s.client.Pipeline()
	for index, entryKey := range entryKeys {
		ttl, live := redisTTL(entries[index])
		if !live {
			pipeline.Del(ctx, entryKey)
			continue
		}
		envelope, err := s.encoder.Encode(entries[index], generation)
		if err != nil {
			return err
		}
		pipeline.Set(ctx, entryKey, envelope, ttl)
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("cache redis set many: %w", err)
	}
	return nil
}

// InvalidateMany deletes keys in one pipeline round trip.
func (s *Store[K, V]) InvalidateMany(ctx context.Context, namespace string, keys []K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	entryKeys, err := s.entryKeys(namespace, keys)
	if err != nil {
		return err
	}
	pipeline := s.client.Pipeline()
	for _, entryKey := range entryKeys {
		pipeline.Del(ctx, entryKey)
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("cache redis invalidate many: %w", err)
	}
	return nil
}

func (s *Store[K, V]) entryKeys(namespace string, keys []K) ([]string, error) {
	entryKeys := make([]string, len(keys))
	for index, key := range keys {
		entryKey, _, err := s.keys(namespace, key)
		if err != nil {
			return nil, err
		}
		entryKeys[index] = entryKey
	}
	return entryKeys, nil
}

// redisTTL returns the native TTL for entry, where zero means no expiration,
// and whether the entry is still live.
func redisTTL[V any](entry cache.Entry[V]) (time.Duration, bool) {
	if entry.NeverExpires() {
		return 0, true
	}
	ttl := time.Until(entry.ExpiresAt)
	return ttl, ttl > 0
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

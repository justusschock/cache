// Package memcached provides a Memcached-backed cache store.
package memcached

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
)

var (
	ErrNilClient = errors.New("cache memcached: client must not be nil")
	ErrNilCodec  = errors.New("cache memcached: codec must not be nil")
)

type config[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures Memcached serialization.
type Option[K comparable, V any] func(*config[K, V]) error

// WithKeyCodec overrides automatic key serialization.
func WithKeyCodec[K comparable, V any](codec cache.Codec[K]) Option[K, V] {
	return func(config *config[K, V]) error {
		if codec == nil {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// WithValueCodec overrides automatic value serialization.
func WithValueCodec[K comparable, V any](codec cache.Codec[V]) Option[K, V] {
	return func(config *config[K, V]) error {
		if codec == nil {
			return ErrNilCodec
		}
		config.valueCodec = codec
		return nil
	}
}

// Store persists entries in Memcached. The caller retains ownership of the client.
type Store[K comparable, V any] struct {
	client  *memcache.Client
	encoder *wire.Encoder[K, V]
}

// New constructs a Memcached store using a caller-owned client.
func New[K comparable, V any](client *memcache.Client, options ...Option[K, V]) (*Store[K, V], error) {
	config := config[K, V]{keyCodec: cache.DefaultCodec[K](), valueCodec: cache.DefaultCodec[V]()}
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
	item, err := s.client.Get(entryKey)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("cache memcached get: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	generation, err := s.generation(generationKey)
	if err != nil {
		return zero, false, err
	}
	entry, recordGeneration, err := s.encoder.Decode(item.Value)
	if err != nil {
		return zero, false, fmt.Errorf("cache memcached decode: %w", err)
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
	if !entry.ExpiresAt.After(time.Now()) {
		return s.Invalidate(ctx, namespace, key)
	}
	generation, err := s.generation(generationKey)
	if err != nil {
		return err
	}
	envelope, err := s.encoder.Encode(entry, generation)
	if err != nil {
		return err
	}
	item := &memcache.Item{Key: entryKey, Value: envelope, Expiration: expiration(entry.ExpiresAt)}
	if err := s.client.Set(item); err != nil {
		return fmt.Errorf("cache memcached set: %w", err)
	}
	return ctx.Err()
}

func (s *Store[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryKey, _, err := s.keys(namespace, key)
	if err != nil {
		return err
	}
	err = s.client.Delete(entryKey)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cache memcached invalidate: %w", err)
	}
	return ctx.Err()
}

func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := memcachedGenerationKey(namespace)
	_, err := s.client.Increment(key, 1)
	if errors.Is(err, memcache.ErrCacheMiss) {
		err = s.client.Add(&memcache.Item{Key: key, Value: []byte("1")})
		if errors.Is(err, memcache.ErrNotStored) {
			_, err = s.client.Increment(key, 1)
		}
	}
	if err != nil {
		return fmt.Errorf("cache memcached invalidate all: %w", err)
	}
	return ctx.Err()
}

func (s *Store[K, V]) keys(namespace string, key K) (string, string, error) {
	physicalKey, err := s.encoder.Key(key)
	if err != nil {
		return "", "", err
	}
	namespaceHash := wire.Namespace(namespace)
	namespacePart := hex.EncodeToString(namespaceHash[:])
	return "gc:e:" + namespacePart + ":" + hex.EncodeToString(physicalKey[:]), "gc:g:" + namespacePart, nil
}

func memcachedGenerationKey(namespace string) string {
	namespaceHash := wire.Namespace(namespace)
	return "gc:g:" + hex.EncodeToString(namespaceHash[:])
}

func (s *Store[K, V]) generation(key string) (uint64, error) {
	item, err := s.client.Get(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cache memcached generation: %w", err)
	}
	generation, err := strconv.ParseUint(string(item.Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cache memcached generation decode: %w", err)
	}
	return generation, nil
}

func expiration(expiresAt time.Time) int32 {
	seconds := expiresAt.Unix()
	if seconds > math.MaxInt32 {
		return math.MaxInt32
	}
	if seconds < 1 {
		return 1
	}
	return int32(seconds)
}

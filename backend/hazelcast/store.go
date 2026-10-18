// Package hazelcast provides a Hazelcast IMap-backed cache store.
package hazelcast

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	hazelcastclient "github.com/hazelcast/hazelcast-go-client"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
)

var (
	ErrNilClient    = errors.New("cache hazelcast: client must not be nil")
	ErrNilCodec     = errors.New("cache hazelcast: codec must not be nil")
	ErrEmptyMapName = errors.New("cache hazelcast: map name must not be empty")
)

type config[K comparable, V any] struct {
	mapName    string
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures Hazelcast storage.
type Option[K comparable, V any] func(*config[K, V]) error

// WithMapName overrides the shared Hazelcast map name.
func WithMapName[K comparable, V any](name string) Option[K, V] {
	return func(config *config[K, V]) error {
		if name == "" {
			return ErrEmptyMapName
		}
		config.mapName = name
		return nil
	}
}

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

// Store persists entries in a Hazelcast map. The caller owns the client.
type Store[K comparable, V any] struct {
	entries *hazelcastclient.Map
	encoder *wire.Encoder[K, V]
}

// New obtains the configured map from a caller-owned Hazelcast client.
func New[K comparable, V any](ctx context.Context, client *hazelcastclient.Client, options ...Option[K, V]) (*Store[K, V], error) {
	config := config[K, V]{mapName: "go-cache", keyCodec: cache.DefaultCodec[K](), valueCodec: cache.DefaultCodec[V]()}
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
	entries, err := client.GetMap(ctx, config.mapName)
	if err != nil {
		return nil, fmt.Errorf("cache hazelcast get map: %w", err)
	}
	return &Store[K, V]{entries: entries, encoder: wire.New(config.keyCodec, config.valueCodec)}, nil
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
	stored, err := s.entries.Get(ctx, entryKey)
	if err != nil {
		return zero, false, fmt.Errorf("cache hazelcast get: %w", err)
	}
	if stored == nil {
		return zero, false, nil
	}
	envelope, ok := stored.([]byte)
	if !ok {
		return zero, false, fmt.Errorf("cache hazelcast: unexpected entry type %T", stored)
	}
	generation, err := s.generation(ctx, generationKey)
	if err != nil {
		return zero, false, err
	}
	entry, recordGeneration, err := s.encoder.Decode(envelope)
	if err != nil {
		return zero, false, fmt.Errorf("cache hazelcast decode: %w", err)
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
	if entry.Expired(time.Now()) {
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
	if entry.NeverExpires() {
		err = s.entries.Set(ctx, entryKey, envelope)
	} else {
		err = s.entries.SetWithTTL(ctx, entryKey, envelope, time.Until(entry.ExpiresAt))
	}
	if err != nil {
		return fmt.Errorf("cache hazelcast set: %w", err)
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
	if err := s.entries.Delete(ctx, entryKey); err != nil {
		return fmt.Errorf("cache hazelcast invalidate: %w", err)
	}
	return nil
}

// InvalidateAll hides every entry in namespace by advancing the namespace
// generation. Older records are ignored on read and left to expire or be
// overwritten.
func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := hazelcastGenerationKey(namespace)
	if err := s.entries.Lock(ctx, key); err != nil {
		return fmt.Errorf("cache hazelcast generation lock: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, s.entries.Unlock(context.WithoutCancel(ctx), key))
	}()
	generation, err := s.generation(ctx, key)
	if err != nil {
		return err
	}
	if err := s.entries.Set(ctx, key, int64(generation+1)); err != nil {
		return fmt.Errorf("cache hazelcast invalidate all: %w", err)
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

func hazelcastGenerationKey(namespace string) string {
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:g:" + hex.EncodeToString(namespaceHash[:])
}

func (s *Store[K, V]) generation(ctx context.Context, key string) (uint64, error) {
	stored, err := s.entries.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("cache hazelcast generation: %w", err)
	}
	if stored == nil {
		return 0, nil
	}
	switch value := stored.(type) {
	case int64:
		if value < 0 {
			return 0, errors.New("cache hazelcast: negative generation")
		}
		return uint64(value), nil
	case int:
		if value < 0 {
			return 0, errors.New("cache hazelcast: negative generation")
		}
		return uint64(value), nil
	default:
		return 0, fmt.Errorf("cache hazelcast: unexpected generation type %T", stored)
	}
}

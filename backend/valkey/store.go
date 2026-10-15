// Package valkey provides a Valkey-backed cache store.
package valkey

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	valkeyclient "github.com/valkey-io/valkey-go"
)

var (
	ErrNilClient = errors.New("cache valkey: client must not be nil")
	ErrNilCodec  = errors.New("cache valkey: codec must not be nil")
)

type config[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures Valkey serialization.
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

// Store persists entries in Valkey. The caller retains ownership of the client.
type Store[K comparable, V any] struct {
	client  valkeyclient.Client
	encoder *wire.Encoder[K, V]
}

// New constructs a Valkey store using a caller-owned client.
func New[K comparable, V any](client valkeyclient.Client, options ...Option[K, V]) (*Store[K, V], error) {
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
	encoded, err := s.client.Do(ctx, s.client.B().Get().Key(entryKey).Build()).ToString()
	if valkeyclient.IsValkeyNil(err) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("cache valkey get: %w", err)
	}
	generation, err := s.generation(ctx, generationKey)
	if err != nil {
		return zero, false, err
	}
	entry, recordGeneration, err := s.encoder.Decode([]byte(encoded))
	if err != nil {
		return zero, false, fmt.Errorf("cache valkey decode: %w", err)
	}
	if recordGeneration != generation || entry.Expired(time.Now()) {
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
	if entry.Expired(time.Now()) {
		return s.Invalidate(ctx, namespace, key)
	}
	generation, err := s.generation(ctx, generationKey)
	if err != nil {
		return err
	}
	command, err := s.setCommand(entryKey, entry, generation)
	if err != nil {
		return err
	}
	if err := s.client.Do(ctx, command).Error(); err != nil {
		return fmt.Errorf("cache valkey set: %w", err)
	}
	return nil
}

// GetMany reads keys and the namespace generation in one pipelined round
// trip. Results are aligned with keys.
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
	commands := make(valkeyclient.Commands, 0, len(entryKeys)+1)
	for _, entryKey := range entryKeys {
		commands = append(commands, s.client.B().Get().Key(entryKey).Build())
	}
	commands = append(commands, s.client.B().Get().Key(valkeyGenerationKey(namespace)).Build())
	results := s.client.DoMulti(ctx, commands...)
	if len(results) != len(commands) {
		return nil, nil, fmt.Errorf("cache valkey get many: %w", cache.ErrBatchLength)
	}
	var generation uint64
	encodedGeneration, err := results[len(entryKeys)].ToString()
	switch {
	case valkeyclient.IsValkeyNil(err):
	case err != nil:
		return nil, nil, fmt.Errorf("cache valkey generation: %w", err)
	default:
		generation, err = strconv.ParseUint(encodedGeneration, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("cache valkey generation decode: %w", err)
		}
	}
	now := time.Now()
	for index := range entryKeys {
		encoded, err := results[index].ToString()
		if valkeyclient.IsValkeyNil(err) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("cache valkey get many: %w", err)
		}
		entry, recordGeneration, err := s.encoder.Decode([]byte(encoded))
		if err != nil {
			return nil, nil, fmt.Errorf("cache valkey decode: %w", err)
		}
		if recordGeneration != generation || entry.Expired(now) {
			continue
		}
		entries[index], present[index] = entry, true
	}
	return entries, present, nil
}

// SetMany writes entries in one pipelined round trip after reading the
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
	generation, err := s.generation(ctx, valkeyGenerationKey(namespace))
	if err != nil {
		return err
	}
	now := time.Now()
	commands := make(valkeyclient.Commands, 0, len(entryKeys))
	for index, entryKey := range entryKeys {
		if entries[index].Expired(now) {
			commands = append(commands, s.client.B().Del().Key(entryKey).Build())
			continue
		}
		command, err := s.setCommand(entryKey, entries[index], generation)
		if err != nil {
			return err
		}
		commands = append(commands, command)
	}
	for _, result := range s.client.DoMulti(ctx, commands...) {
		if err := result.Error(); err != nil {
			return fmt.Errorf("cache valkey set many: %w", err)
		}
	}
	return nil
}

// InvalidateMany deletes keys in one pipelined round trip.
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
	commands := make(valkeyclient.Commands, 0, len(entryKeys))
	for _, entryKey := range entryKeys {
		commands = append(commands, s.client.B().Del().Key(entryKey).Build())
	}
	for _, result := range s.client.DoMulti(ctx, commands...) {
		if err := result.Error(); err != nil {
			return fmt.Errorf("cache valkey invalidate many: %w", err)
		}
	}
	return nil
}

// setCommand builds a SET for a live entry. Entries without expiration get no
// native TTL; others use a millisecond TTL of at least one millisecond.
func (s *Store[K, V]) setCommand(entryKey string, entry cache.Entry[V], generation uint64) (valkeyclient.Completed, error) {
	envelope, err := s.encoder.Encode(entry, generation)
	if err != nil {
		return valkeyclient.Completed{}, err
	}
	value := s.client.B().Set().Key(entryKey).Value(string(envelope))
	if entry.NeverExpires() {
		return value.Build(), nil
	}
	milliseconds := time.Until(entry.ExpiresAt).Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	return value.PxMilliseconds(milliseconds).Build(), nil
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

func (s *Store[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryKey, _, err := s.keys(namespace, key)
	if err != nil {
		return err
	}
	if err := s.client.Do(ctx, s.client.B().Del().Key(entryKey).Build()).Error(); err != nil {
		return fmt.Errorf("cache valkey invalidate: %w", err)
	}
	return nil
}

func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := valkeyGenerationKey(namespace)
	if err := s.client.Do(ctx, s.client.B().Incr().Key(key).Build()).Error(); err != nil {
		return fmt.Errorf("cache valkey invalidate all: %w", err)
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

func valkeyGenerationKey(namespace string) string {
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:g:" + hex.EncodeToString(namespaceHash[:])
}

func (s *Store[K, V]) generation(ctx context.Context, key string) (uint64, error) {
	encoded, err := s.client.Do(ctx, s.client.B().Get().Key(key).Build()).ToString()
	if valkeyclient.IsValkeyNil(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cache valkey generation: %w", err)
	}
	generation, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cache valkey generation decode: %w", err)
	}
	return generation, nil
}

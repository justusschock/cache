// Package disk provides a persistent bbolt-backed cache store.
package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrEmptyPath = errors.New("cache disk: path must not be empty")
	ErrNilCodec  = errors.New("cache disk: codec must not be nil")
)

var (
	rootBucket    = []byte("go-cache")
	entriesBucket = []byte("entries")
	generationKey = []byte("generation")
)

type config[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures disk serialization.
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

// Store persists entries in a bbolt database owned by the store.
type Store[K comparable, V any] struct {
	db      *bolt.DB
	encoder *wire.Encoder[K, V]

	closeOnce sync.Once
	closeErr  error
}

// Open opens or creates an owner-only bbolt cache file.
func Open[K comparable, V any](path string, options ...Option[K, V]) (*Store[K, V], error) {
	if path == "" {
		return nil, ErrEmptyPath
	}
	config := config[K, V]{keyCodec: cache.DefaultCodec[K](), valueCodec: cache.DefaultCodec[V]()}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("cache disk open: %w", err)
	}
	return &Store[K, V]{db: db, encoder: wire.New(config.keyCodec, config.valueCodec)}, nil
}

func (s *Store[K, V]) Get(ctx context.Context, namespace string, key K) (cache.Entry[V], bool, error) {
	var entry cache.Entry[V]
	if err := ctx.Err(); err != nil {
		return entry, false, err
	}
	physicalKey, err := s.encoder.Key(key)
	if err != nil {
		return entry, false, err
	}
	namespaceKey := wire.Namespace(namespace)
	var present bool
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		root := tx.Bucket(rootBucket)
		if root == nil {
			return nil
		}
		namespaceBucket := root.Bucket(namespaceKey[:])
		if namespaceBucket == nil {
			return nil
		}
		entries := namespaceBucket.Bucket(entriesBucket)
		if entries == nil {
			return nil
		}
		envelope := entries.Get(physicalKey[:])
		if envelope == nil {
			return nil
		}
		decoded, recordGeneration, decodeErr := s.encoder.Decode(envelope)
		if decodeErr != nil {
			return decodeErr
		}
		if recordGeneration != readGeneration(namespaceBucket) || !decoded.ExpiresAt.After(time.Now()) {
			return nil
		}
		entry = decoded
		present = true
		return nil
	})
	if err != nil {
		return cache.Entry[V]{}, false, fmt.Errorf("cache disk get: %w", err)
	}
	return entry, present, nil
}

func (s *Store[K, V]) Set(ctx context.Context, namespace string, key K, entry cache.Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	physicalKey, err := s.encoder.Key(key)
	if err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := tx.CreateBucketIfNotExists(rootBucket)
		if err != nil {
			return err
		}
		namespaceBucket, err := root.CreateBucketIfNotExists(namespaceKey[:])
		if err != nil {
			return err
		}
		entries, err := namespaceBucket.CreateBucketIfNotExists(entriesBucket)
		if err != nil {
			return err
		}
		envelope, err := s.encoder.Encode(entry, readGeneration(namespaceBucket))
		if err != nil {
			return err
		}
		return entries.Put(physicalKey[:], envelope)
	})
	if err != nil {
		return fmt.Errorf("cache disk set: %w", err)
	}
	return nil
}

func (s *Store[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	physicalKey, err := s.encoder.Key(key)
	if err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		root := tx.Bucket(rootBucket)
		if root == nil {
			return nil
		}
		namespaceBucket := root.Bucket(namespaceKey[:])
		if namespaceBucket == nil {
			return nil
		}
		entries := namespaceBucket.Bucket(entriesBucket)
		if entries == nil {
			return nil
		}
		return entries.Delete(physicalKey[:])
	})
	if err != nil {
		return fmt.Errorf("cache disk invalidate: %w", err)
	}
	return nil
}

func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := tx.CreateBucketIfNotExists(rootBucket)
		if err != nil {
			return err
		}
		namespaceBucket, err := root.CreateBucketIfNotExists(namespaceKey[:])
		if err != nil {
			return err
		}
		generation := readGeneration(namespaceBucket) + 1
		encodedGeneration := make([]byte, 8)
		binary.BigEndian.PutUint64(encodedGeneration, generation)
		if err := namespaceBucket.Put(generationKey, encodedGeneration); err != nil {
			return err
		}
		if namespaceBucket.Bucket(entriesBucket) != nil {
			if err := namespaceBucket.DeleteBucket(entriesBucket); err != nil {
				return err
			}
		}
		_, err = namespaceBucket.CreateBucket(entriesBucket)
		return err
	})
	if err != nil {
		return fmt.Errorf("cache disk invalidate all: %w", err)
	}
	return nil
}

// Close closes the bbolt database opened by Open.
func (s *Store[K, V]) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func readGeneration(bucket *bolt.Bucket) uint64 {
	encoded := bucket.Get(generationKey)
	if len(encoded) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(encoded)
}

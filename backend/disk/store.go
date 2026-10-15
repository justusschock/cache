// Package disk provides a persistent bbolt-backed cache store.
package disk

import (
	"bytes"
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
	ErrEmptyPath           = errors.New("cache disk: path must not be empty")
	ErrNilCodec            = errors.New("cache disk: codec must not be nil")
	ErrNilScanFunc         = errors.New("cache disk: scan function must not be nil")
	ErrOrderedKeysRequired = errors.New("cache disk: prefix operations require WithOrderedKeys")
)

var (
	rootBucket           = []byte("go-cache")
	entriesBucket        = []byte("entries")
	orderedEntriesBucket = []byte("ordered-entries")
	generationKey        = []byte("generation")
)

// orderedKeyTag precedes every ordered physical key so that an empty encoded
// key still forms a valid, non-empty bbolt key.
const orderedKeyTag byte = 'k'

// contextCheckInterval bounds how many records prefix operations visit
// between context cancellation checks.
const contextCheckInterval = 1024

type config[K comparable, V any] struct {
	keyCodec    cache.Codec[K]
	valueCodec  cache.Codec[V]
	orderedKeys bool
}

// Option configures disk serialization.
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

// WithOrderedKeys stores the key codec's bytes verbatim instead of their
// SHA-256 digest. bbolt then keeps entries in encoded-key byte order, which
// enables ScanPrefix and DeletePrefix and skips per-key hashing.
//
// Ordered keys are a separate on-disk layout: entries written without this
// option are not visible with it and vice versa, so a file must always be
// opened with the same choice. Encoded keys must stay below bbolt's maximum
// key size of 32 KiB; the store adds one tag byte.
func WithOrderedKeys[K comparable, V any]() Option[K, V] {
	return func(config *config[K, V]) error {
		config.orderedKeys = true
		return nil
	}
}

// Store persists entries in a bbolt database owned by the store.
//
// Store implements cache.BatchStore. Each batch call runs in a single bbolt
// transaction, so a batch is applied atomically and costs one commit.
type Store[K comparable, V any] struct {
	db          *bolt.DB
	encoder     *wire.Encoder[K, V]
	keyCodec    cache.Codec[K]
	orderedKeys bool

	closeOnce sync.Once
	closeErr  error
}

var _ cache.BatchStore[string, int] = (*Store[string, int])(nil)

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
	return &Store[K, V]{
		db:          db,
		encoder:     wire.New(config.keyCodec, config.valueCodec),
		keyCodec:    config.keyCodec,
		orderedKeys: config.orderedKeys,
	}, nil
}

func (s *Store[K, V]) Get(ctx context.Context, namespace string, key K) (cache.Entry[V], bool, error) {
	var entry cache.Entry[V]
	if err := ctx.Err(); err != nil {
		return entry, false, err
	}
	physicalKey, err := s.physicalKey(key)
	if err != nil {
		return entry, false, err
	}
	namespaceKey := wire.Namespace(namespace)
	var present bool
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		namespaceBucket, entries := s.readBuckets(tx, namespaceKey)
		if entries == nil {
			return nil
		}
		var lookupErr error
		entry, present, lookupErr = s.lookup(entries, readGeneration(namespaceBucket), physicalKey, time.Now())
		return lookupErr
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
	physicalKey, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		namespaceBucket, entries, err := s.writeBuckets(tx, namespaceKey)
		if err != nil {
			return err
		}
		envelope, err := s.encoder.Encode(entry, readGeneration(namespaceBucket))
		if err != nil {
			return err
		}
		return entries.Put(physicalKey, envelope)
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
	physicalKey, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, entries := s.readBuckets(tx, namespaceKey)
		if entries == nil {
			return nil
		}
		return entries.Delete(physicalKey)
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
		for _, name := range [][]byte{entriesBucket, orderedEntriesBucket} {
			if namespaceBucket.Bucket(name) != nil {
				if err := namespaceBucket.DeleteBucket(name); err != nil {
					return err
				}
			}
		}
		_, err = namespaceBucket.CreateBucket(s.entriesBucketName())
		return err
	})
	if err != nil {
		return fmt.Errorf("cache disk invalidate all: %w", err)
	}
	return nil
}

// GetMany looks up keys in one read transaction. Results are aligned with keys.
func (s *Store[K, V]) GetMany(ctx context.Context, namespace string, keys []K) ([]cache.Entry[V], []bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	physicalKeys, err := s.physicalKeys(keys)
	if err != nil {
		return nil, nil, err
	}
	entries := make([]cache.Entry[V], len(keys))
	present := make([]bool, len(keys))
	namespaceKey := wire.Namespace(namespace)
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		namespaceBucket, bucket := s.readBuckets(tx, namespaceKey)
		if bucket == nil {
			return nil
		}
		generation := readGeneration(namespaceBucket)
		now := time.Now()
		for index, physicalKey := range physicalKeys {
			entry, ok, err := s.lookup(bucket, generation, physicalKey, now)
			if err != nil {
				return err
			}
			entries[index], present[index] = entry, ok
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("cache disk get many: %w", err)
	}
	return entries, present, nil
}

// SetMany stores entries[i] under keys[i] in one atomic write transaction.
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
	physicalKeys, err := s.physicalKeys(keys)
	if err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		namespaceBucket, bucket, err := s.writeBuckets(tx, namespaceKey)
		if err != nil {
			return err
		}
		generation := readGeneration(namespaceBucket)
		for index, physicalKey := range physicalKeys {
			envelope, err := s.encoder.Encode(entries[index], generation)
			if err != nil {
				return err
			}
			if err := bucket.Put(physicalKey, envelope); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cache disk set many: %w", err)
	}
	return nil
}

// InvalidateMany removes keys in one atomic write transaction.
func (s *Store[K, V]) InvalidateMany(ctx context.Context, namespace string, keys []K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	physicalKeys, err := s.physicalKeys(keys)
	if err != nil {
		return err
	}
	namespaceKey := wire.Namespace(namespace)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, bucket := s.readBuckets(tx, namespaceKey)
		if bucket == nil {
			return nil
		}
		for _, physicalKey := range physicalKeys {
			if err := bucket.Delete(physicalKey); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cache disk invalidate many: %w", err)
	}
	return nil
}

// ScanPrefix calls fn for every live entry in namespace whose encoded key
// starts with prefix, in ascending encoded-key byte order, until fn returns
// false. Expired entries and entries written before the last InvalidateAll
// are skipped. An empty prefix visits the whole namespace.
//
// prefix is matched against the bytes produced by the store's key codec, not
// against K itself. With the default codec a string key encodes to its raw
// bytes and a []byte key to itself, so prefix is a plain string or byte
// prefix; integers encode as decimal text (lexicographic, not numeric, order)
// and composite keys as JSON. Supply a custom key codec when another ordering
// is required. The store must be opened with WithOrderedKeys; otherwise
// ScanPrefix returns ErrOrderedKeysRequired.
//
// The scan runs inside one bbolt read transaction that stays open while fn
// executes, so fn must not write to the same store. Key and value codecs must
// not retain the byte slices they decode.
func (s *Store[K, V]) ScanPrefix(ctx context.Context, namespace string, prefix []byte, fn func(key K, entry cache.Entry[V]) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.orderedKeys {
		return ErrOrderedKeysRequired
	}
	if fn == nil {
		return ErrNilScanFunc
	}
	physicalPrefix := orderedKey(prefix)
	namespaceKey := wire.Namespace(namespace)
	err := s.db.View(func(tx *bolt.Tx) error {
		namespaceBucket, bucket := s.readBuckets(tx, namespaceKey)
		if bucket == nil {
			return ctx.Err()
		}
		generation := readGeneration(namespaceBucket)
		now := time.Now()
		cursor := bucket.Cursor()
		visited := 0
		for physicalKey, envelope := cursor.Seek(physicalPrefix); physicalKey != nil && bytes.HasPrefix(physicalKey, physicalPrefix); physicalKey, envelope = cursor.Next() {
			if visited%contextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			visited++
			entry, recordGeneration, err := s.encoder.Decode(envelope)
			if err != nil {
				return err
			}
			if recordGeneration != generation || entry.Expired(now) {
				continue
			}
			key, err := s.keyCodec.Decode(physicalKey[1:])
			if err != nil {
				return fmt.Errorf("cache key decode: %w", err)
			}
			if !fn(key, entry) {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cache disk scan prefix: %w", err)
	}
	return nil
}

// DeletePrefix removes every record in namespace whose encoded key starts with
// prefix in one atomic write transaction. It returns the number of records
// removed, including logically expired ones. Prefix semantics match
// ScanPrefix; an empty prefix removes every entry in the namespace. The store
// must be opened with WithOrderedKeys; otherwise DeletePrefix returns
// ErrOrderedKeysRequired.
func (s *Store[K, V]) DeletePrefix(ctx context.Context, namespace string, prefix []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !s.orderedKeys {
		return 0, ErrOrderedKeysRequired
	}
	physicalPrefix := orderedKey(prefix)
	namespaceKey := wire.Namespace(namespace)
	removed := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		removed = 0
		_, bucket := s.readBuckets(tx, namespaceKey)
		if bucket == nil {
			return ctx.Err()
		}
		cursor := bucket.Cursor()
		// Seek again after every delete: advancing a bbolt cursor after Delete can skip records.
		for physicalKey, _ := cursor.Seek(physicalPrefix); physicalKey != nil && bytes.HasPrefix(physicalKey, physicalPrefix); physicalKey, _ = cursor.Seek(physicalPrefix) {
			if removed%contextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("cache disk delete prefix: %w", err)
	}
	return removed, nil
}

// Close closes the bbolt database opened by Open.
func (s *Store[K, V]) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func (s *Store[K, V]) lookup(bucket *bolt.Bucket, generation uint64, physicalKey []byte, now time.Time) (cache.Entry[V], bool, error) {
	var zero cache.Entry[V]
	envelope := bucket.Get(physicalKey)
	if envelope == nil {
		return zero, false, nil
	}
	decoded, recordGeneration, err := s.encoder.Decode(envelope)
	if err != nil {
		return zero, false, err
	}
	if recordGeneration != generation || decoded.Expired(now) {
		return zero, false, nil
	}
	return decoded, true, nil
}

func (s *Store[K, V]) readBuckets(tx *bolt.Tx, namespaceKey [32]byte) (*bolt.Bucket, *bolt.Bucket) {
	root := tx.Bucket(rootBucket)
	if root == nil {
		return nil, nil
	}
	namespaceBucket := root.Bucket(namespaceKey[:])
	if namespaceBucket == nil {
		return nil, nil
	}
	return namespaceBucket, namespaceBucket.Bucket(s.entriesBucketName())
}

func (s *Store[K, V]) writeBuckets(tx *bolt.Tx, namespaceKey [32]byte) (*bolt.Bucket, *bolt.Bucket, error) {
	root, err := tx.CreateBucketIfNotExists(rootBucket)
	if err != nil {
		return nil, nil, err
	}
	namespaceBucket, err := root.CreateBucketIfNotExists(namespaceKey[:])
	if err != nil {
		return nil, nil, err
	}
	entries, err := namespaceBucket.CreateBucketIfNotExists(s.entriesBucketName())
	if err != nil {
		return nil, nil, err
	}
	return namespaceBucket, entries, nil
}

func (s *Store[K, V]) entriesBucketName() []byte {
	if s.orderedKeys {
		return orderedEntriesBucket
	}
	return entriesBucket
}

func (s *Store[K, V]) physicalKey(key K) ([]byte, error) {
	if !s.orderedKeys {
		hashed, err := s.encoder.Key(key)
		if err != nil {
			return nil, err
		}
		return hashed[:], nil
	}
	encoded, err := s.keyCodec.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("cache key encode: %w", err)
	}
	return orderedKey(encoded), nil
}

func (s *Store[K, V]) physicalKeys(keys []K) ([][]byte, error) {
	physicalKeys := make([][]byte, len(keys))
	for index, key := range keys {
		physicalKey, err := s.physicalKey(key)
		if err != nil {
			return nil, err
		}
		physicalKeys[index] = physicalKey
	}
	return physicalKeys, nil
}

func orderedKey(encoded []byte) []byte {
	physical := make([]byte, 1+len(encoded))
	physical[0] = orderedKeyTag
	copy(physical[1:], encoded)
	return physical
}

func readGeneration(bucket *bolt.Bucket) uint64 {
	encoded := bucket.Get(generationKey)
	if len(encoded) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(encoded)
}

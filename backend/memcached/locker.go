package memcached

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
)

type lockerConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// LockerOption configures Memcached lock key serialization.
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

// Locker provides renewable Memcached leases using add and CAS.
type Locker[K comparable] struct {
	client   *memcache.Client
	keyCodec cache.Codec[K]
}

// NewLocker constructs a Memcached locker using a caller-owned client.
func NewLocker[K comparable](client *memcache.Client, options ...LockerOption[K]) (*Locker[K], error) {
	config := lockerConfig[K]{keyCodec: cache.DefaultCodec[K]()}
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
	return &Locker[K]{client: client, keyCodec: config.keyCodec}, nil
}

func (l *Locker[K]) Lock(ctx context.Context, namespace string, key K, ttl time.Duration) (cache.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ttl <= 0 {
		return nil, cache.ErrInvalidTTL
	}
	physicalKey, err := l.key(namespace, key)
	if err != nil {
		return nil, err
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("cache memcached lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	err = l.client.Add(&memcache.Item{Key: physicalKey, Value: []byte(token), Expiration: expiration(time.Now().Add(ttl))})
	if errors.Is(err, memcache.ErrNotStored) {
		return nil, cache.ErrLockNotAcquired
	}
	if err != nil {
		return nil, fmt.Errorf("cache memcached lock: %w", err)
	}
	lease := &memcachedLease{client: l.client, key: physicalKey, token: token}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, lease.Release(context.Background()))
	}
	return lease, nil
}

func (l *Locker[K]) key(namespace string, key K) (string, error) {
	encoded, err := l.keyCodec.Encode(key)
	if err != nil {
		return "", fmt.Errorf("cache memcached lock key: %w", err)
	}
	keyHash := sha256.Sum256(encoded)
	namespaceHash := wire.Namespace(namespace)
	return "gc:l:" + hex.EncodeToString(namespaceHash[:]) + ":" + hex.EncodeToString(keyHash[:]), nil
}

type memcachedLease struct {
	client *memcache.Client
	key    string
	token  string
}

func (l *memcachedLease) Refresh(ctx context.Context, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	item, err := l.client.Get(l.key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return cache.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("cache memcached get lease: %w", err)
	}
	if string(item.Value) != l.token {
		return cache.ErrLeaseLost
	}
	item.Expiration = expiration(time.Now().Add(ttl))
	if err := l.client.CompareAndSwap(item); err != nil {
		if errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) || errors.Is(err, memcache.ErrCacheMiss) {
			return cache.ErrLeaseLost
		}
		return fmt.Errorf("cache memcached refresh lease: %w", err)
	}
	return ctx.Err()
}

func (l *memcachedLease) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	item, err := l.client.Get(l.key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return cache.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("cache memcached get lease: %w", err)
	}
	if string(item.Value) != l.token {
		return cache.ErrLeaseLost
	}
	item.Value = []byte("released")
	item.Expiration = expiration(time.Now().Add(time.Second))
	if err := l.client.CompareAndSwap(item); err != nil {
		if errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) || errors.Is(err, memcache.ErrCacheMiss) {
			return cache.ErrLeaseLost
		}
		return fmt.Errorf("cache memcached release lease: %w", err)
	}
	if err := l.client.Delete(l.key); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return fmt.Errorf("cache memcached delete lease: %w", err)
	}
	return ctx.Err()
}

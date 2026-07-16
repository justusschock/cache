package hazelcast

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	hazelcastclient "github.com/hazelcast/hazelcast-go-client"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
)

type lockerConfig[K comparable] struct {
	mapName  string
	keyCodec cache.Codec[K]
}

// LockerOption configures Hazelcast lease storage.
type LockerOption[K comparable] func(*lockerConfig[K]) error

// WithLockerMapName overrides the Hazelcast map used for leases.
func WithLockerMapName[K comparable](name string) LockerOption[K] {
	return func(config *lockerConfig[K]) error {
		if name == "" {
			return ErrEmptyMapName
		}
		config.mapName = name
		return nil
	}
}

// WithLockerKeyCodec overrides automatic lock key serialization.
func WithLockerKeyCodec[K comparable](codec cache.Codec[K]) LockerOption[K] {
	return func(config *lockerConfig[K]) error {
		if codec == nil {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// Locker provides token-checked renewable Hazelcast map leases.
type Locker[K comparable] struct {
	locks    *hazelcastclient.Map
	keyCodec cache.Codec[K]
}

// NewLocker obtains a lease map from a caller-owned Hazelcast client.
func NewLocker[K comparable](ctx context.Context, client *hazelcastclient.Client, options ...LockerOption[K]) (*Locker[K], error) {
	config := lockerConfig[K]{mapName: "go-cache-locks", keyCodec: cache.DefaultCodec[K]()}
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
	locks, err := client.GetMap(ctx, config.mapName)
	if err != nil {
		return nil, fmt.Errorf("cache hazelcast get lock map: %w", err)
	}
	return &Locker[K]{locks: locks, keyCodec: config.keyCodec}, nil
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
		return nil, fmt.Errorf("cache hazelcast lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	previous, err := l.locks.PutIfAbsentWithTTL(ctx, physicalKey, token, ttl)
	if err != nil {
		return nil, fmt.Errorf("cache hazelcast lock: %w", err)
	}
	if previous != nil {
		return nil, cache.ErrLockNotAcquired
	}
	return &hazelcastLease{locks: l.locks, key: physicalKey, token: token}, nil
}

func (l *Locker[K]) key(namespace string, key K) (string, error) {
	encoded, err := l.keyCodec.Encode(key)
	if err != nil {
		return "", fmt.Errorf("cache hazelcast lock key: %w", err)
	}
	keyHash := sha256.Sum256(encoded)
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:l:" + hex.EncodeToString(namespaceHash[:]) + ":" + hex.EncodeToString(keyHash[:]), nil
}

type hazelcastLease struct {
	locks *hazelcastclient.Map
	key   string
	token string
}

func (l *hazelcastLease) Refresh(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	owned, err := l.locks.ReplaceIfSame(ctx, l.key, l.token, l.token)
	if err != nil {
		return fmt.Errorf("cache hazelcast refresh lease: %w", err)
	}
	if !owned {
		return cache.ErrLeaseLost
	}
	affected, err := l.locks.SetTTLAffected(ctx, l.key, ttl)
	if err != nil {
		return fmt.Errorf("cache hazelcast refresh lease ttl: %w", err)
	}
	if !affected {
		return cache.ErrLeaseLost
	}
	return nil
}

func (l *hazelcastLease) Release(ctx context.Context) error {
	removed, err := l.locks.RemoveIfSame(ctx, l.key, l.token)
	if err != nil {
		return fmt.Errorf("cache hazelcast release lease: %w", err)
	}
	if !removed {
		return cache.ErrLeaseLost
	}
	return nil
}

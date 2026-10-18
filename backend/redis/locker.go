package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	goredis "github.com/redis/go-redis/v9"
)

var (
	refreshScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)
	releaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`)
)

type lockerConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// LockerOption configures Redis lock key serialization.
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

// Locker provides renewable Redis leases for cache-fill coordination.
type Locker[K comparable] struct {
	client   goredis.UniversalClient
	keyCodec cache.Codec[K]
}

// NewLocker constructs a Redis locker using a caller-owned client.
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
	if isNil(client) {
		return nil, ErrNilClient
	}
	return &Locker[K]{client: client, keyCodec: config.keyCodec}, nil
}

// Lock acquires a lease on key in namespace for ttl. It returns
// cache.ErrLockNotAcquired while another lease is live and
// cache.ErrInvalidTTL for a non-positive ttl.
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
		return nil, fmt.Errorf("cache redis lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	acquired, err := l.client.SetNX(ctx, physicalKey, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("cache redis lock: %w", err)
	}
	if !acquired {
		return nil, cache.ErrLockNotAcquired
	}
	return &redisLease{client: l.client, key: physicalKey, token: token}, nil
}

func (l *Locker[K]) key(namespace string, key K) (string, error) {
	encoded, err := l.keyCodec.Encode(key)
	if err != nil {
		return "", fmt.Errorf("cache redis lock key: %w", err)
	}
	keyHash := sha256.Sum256(encoded)
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:l:" + hex.EncodeToString(namespaceHash[:]) + ":" + hex.EncodeToString(keyHash[:]), nil
}

type redisLease struct {
	client goredis.UniversalClient
	key    string
	token  string
}

func (l *redisLease) Refresh(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	milliseconds := ttl.Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	refreshed, err := refreshScript.Run(ctx, l.client, []string{l.key}, l.token, milliseconds).Int64()
	if err != nil {
		return fmt.Errorf("cache redis refresh lease: %w", err)
	}
	if refreshed == 0 {
		return cache.ErrLeaseLost
	}
	return nil
}

func (l *redisLease) Release(ctx context.Context) error {
	released, err := releaseScript.Run(ctx, l.client, []string{l.key}, l.token).Int64()
	if err != nil {
		return fmt.Errorf("cache redis release lease: %w", err)
	}
	if released == 0 {
		return cache.ErrLeaseLost
	}
	return nil
}

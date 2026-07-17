package valkey

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	valkeyclient "github.com/valkey-io/valkey-go"
)

const (
	refreshLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`
	releaseLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`
)

type lockerConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// LockerOption configures Valkey lock key serialization.
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

// Locker provides renewable Valkey leases for cache-fill coordination.
type Locker[K comparable] struct {
	client   valkeyclient.Client
	keyCodec cache.Codec[K]
}

// NewLocker constructs a Valkey locker using a caller-owned client.
func NewLocker[K comparable](client valkeyclient.Client, options ...LockerOption[K]) (*Locker[K], error) {
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
		return nil, fmt.Errorf("cache valkey lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	milliseconds := ttl.Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	command := l.client.B().Set().Key(physicalKey).Value(token).Nx().PxMilliseconds(milliseconds).Build()
	_, err = l.client.Do(ctx, command).ToString()
	if valkeyclient.IsValkeyNil(err) {
		return nil, cache.ErrLockNotAcquired
	}
	if err != nil {
		return nil, fmt.Errorf("cache valkey lock: %w", err)
	}
	return &valkeyLease{client: l.client, key: physicalKey, token: token}, nil
}

func (l *Locker[K]) key(namespace string, key K) (string, error) {
	encoded, err := l.keyCodec.Encode(key)
	if err != nil {
		return "", fmt.Errorf("cache valkey lock key: %w", err)
	}
	keyHash := sha256.Sum256(encoded)
	namespaceHash := wire.Namespace(namespace)
	return "go-cache:l:" + hex.EncodeToString(namespaceHash[:]) + ":" + hex.EncodeToString(keyHash[:]), nil
}

type valkeyLease struct {
	client valkeyclient.Client
	key    string
	token  string
}

func (l *valkeyLease) Refresh(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	milliseconds := ttl.Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	command := l.client.B().Eval().Script(refreshLeaseScript).Numkeys(1).Key(l.key).Arg(l.token, strconv.FormatInt(milliseconds, 10)).Build()
	refreshed, err := l.client.Do(ctx, command).ToInt64()
	if err != nil {
		return fmt.Errorf("cache valkey refresh lease: %w", err)
	}
	if refreshed == 0 {
		return cache.ErrLeaseLost
	}
	return nil
}

func (l *valkeyLease) Release(ctx context.Context) error {
	command := l.client.B().Eval().Script(releaseLeaseScript).Numkeys(1).Key(l.key).Arg(l.token).Build()
	released, err := l.client.Do(ctx, command).ToInt64()
	if err != nil {
		return fmt.Errorf("cache valkey release lease: %w", err)
	}
	if released == 0 {
		return cache.ErrLeaseLost
	}
	return nil
}

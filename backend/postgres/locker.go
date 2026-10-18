package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	cache "github.com/justusschock/cache"
	"github.com/justusschock/cache/internal/wire"
)

type lockerConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// LockerOption configures PostgreSQL lock key serialization.
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

// Locker provides renewable PostgreSQL row leases.
type Locker[K comparable] struct {
	pool     *pgxpool.Pool
	keyCodec cache.Codec[K]
}

// NewLocker constructs a PostgreSQL locker using a caller-owned pool.
func NewLocker[K comparable](pool *pgxpool.Pool, options ...LockerOption[K]) (*Locker[K], error) {
	config := lockerConfig[K]{keyCodec: cache.DefaultCodec[K]()}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	if pool == nil {
		return nil, ErrNilPool
	}
	return &Locker[K]{pool: pool, keyCodec: config.keyCodec}, nil
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
	namespaceHash, keyHash, err := l.keys(namespace, key)
	if err != nil {
		return nil, err
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("cache postgres lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	var returnedToken string
	err = l.pool.QueryRow(ctx, `
		INSERT INTO go_cache_locks (namespace_hash, key_hash, token, expires_at)
		VALUES ($1, $2, $3, clock_timestamp() + ($4::double precision * INTERVAL '1 second'))
		ON CONFLICT (namespace_hash, key_hash) DO UPDATE
		SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
		WHERE go_cache_locks.expires_at <= clock_timestamp()
		RETURNING token
	`, namespaceHash, keyHash, token, ttl.Seconds()).Scan(&returnedToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, cache.ErrLockNotAcquired
	}
	if err != nil {
		return nil, fmt.Errorf("cache postgres lock: %w", err)
	}
	return &postgresLease{pool: l.pool, namespaceHash: namespaceHash, keyHash: keyHash, token: token}, nil
}

func (l *Locker[K]) keys(namespace string, key K) ([]byte, []byte, error) {
	encoded, err := l.keyCodec.Encode(key)
	if err != nil {
		return nil, nil, fmt.Errorf("cache postgres lock key: %w", err)
	}
	keyHash := wire.Namespace(string(encoded))
	namespaceHash := wire.Namespace(namespace)
	return namespaceHash[:], keyHash[:], nil
}

type postgresLease struct {
	pool          *pgxpool.Pool
	namespaceHash []byte
	keyHash       []byte
	token         string
}

func (l *postgresLease) Refresh(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	result, err := l.pool.Exec(ctx, `
		UPDATE go_cache_locks
		SET expires_at = clock_timestamp() + ($4::double precision * INTERVAL '1 second')
		WHERE namespace_hash = $1 AND key_hash = $2 AND token = $3
			AND expires_at > clock_timestamp()
	`, l.namespaceHash, l.keyHash, l.token, ttl.Seconds())
	if err != nil {
		return fmt.Errorf("cache postgres refresh lease: %w", err)
	}
	if result.RowsAffected() == 0 {
		return cache.ErrLeaseLost
	}
	return nil
}

func (l *postgresLease) Release(ctx context.Context) error {
	var owned bool
	err := l.pool.QueryRow(ctx, `
		DELETE FROM go_cache_locks
		WHERE namespace_hash = $1 AND key_hash = $2 AND token = $3
		RETURNING expires_at > clock_timestamp()
	`, l.namespaceHash, l.keyHash, l.token).Scan(&owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return cache.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("cache postgres release lease: %w", err)
	}
	if !owned {
		return cache.ErrLeaseLost
	}
	return nil
}

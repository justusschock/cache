// Package postgres provides a PostgreSQL-backed cache store.
package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
)

var (
	ErrNilPool  = errors.New("cache postgres: pool must not be nil")
	ErrNilCodec = errors.New("cache postgres: codec must not be nil")
)

const schema = `
CREATE TABLE IF NOT EXISTS go_cache_generations (
    namespace_hash BYTEA PRIMARY KEY,
    generation BIGINT NOT NULL CHECK (generation >= 0)
);
CREATE TABLE IF NOT EXISTS go_cache_entries (
    namespace_hash BYTEA NOT NULL,
    key_hash BYTEA NOT NULL,
    payload BYTEA NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace_hash, key_hash)
);
CREATE INDEX IF NOT EXISTS go_cache_entries_expires_at_idx
    ON go_cache_entries (expires_at);
CREATE TABLE IF NOT EXISTS go_cache_locks (
    namespace_hash BYTEA NOT NULL,
    key_hash BYTEA NOT NULL,
    token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace_hash, key_hash)
);
CREATE INDEX IF NOT EXISTS go_cache_locks_expires_at_idx
    ON go_cache_locks (expires_at);
`

type config[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

// Option configures PostgreSQL serialization.
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

// Store persists entries in PostgreSQL. The caller retains ownership of the pool.
type Store[K comparable, V any] struct {
	pool    *pgxpool.Pool
	encoder *wire.Encoder[K, V]
}

// New constructs a PostgreSQL store using a caller-owned pool.
func New[K comparable, V any](pool *pgxpool.Pool, options ...Option[K, V]) (*Store[K, V], error) {
	config := config[K, V]{keyCodec: cache.DefaultCodec[K](), valueCodec: cache.DefaultCodec[V]()}
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
	return &Store[K, V]{pool: pool, encoder: wire.New(config.keyCodec, config.valueCodec)}, nil
}

// Migrate creates the idempotent cache tables and expiration index.
func (s *Store[K, V]) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("cache postgres migrate: %w", err)
	}
	return nil
}

func (s *Store[K, V]) Get(ctx context.Context, namespace string, key K) (cache.Entry[V], bool, error) {
	var zero cache.Entry[V]
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	namespaceHash := wire.Namespace(namespace)
	keyHash, err := s.encoder.Key(key)
	if err != nil {
		return zero, false, err
	}
	var payload []byte
	var generation int64
	err = s.pool.QueryRow(ctx, `
		SELECT e.payload, COALESCE(g.generation, 0)
		FROM go_cache_entries e
		LEFT JOIN go_cache_generations g USING (namespace_hash)
		WHERE e.namespace_hash = $1 AND e.key_hash = $2 AND e.expires_at > NOW()
	`, namespaceHash[:], keyHash[:]).Scan(&payload, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("cache postgres get: %w", err)
	}
	entry, recordGeneration, err := s.encoder.Decode(payload)
	if err != nil {
		return zero, false, fmt.Errorf("cache postgres decode: %w", err)
	}
	if generation < 0 || recordGeneration != uint64(generation) {
		return zero, false, nil
	}
	return entry, true, nil
}

func (s *Store[K, V]) Set(ctx context.Context, namespace string, key K, entry cache.Entry[V]) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	namespaceHash := wire.Namespace(namespace)
	keyHash, err := s.encoder.Key(key)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("cache postgres begin set: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, rollback(tx))
	}()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryKey(namespaceHash)); err != nil {
		return fmt.Errorf("cache postgres set lock: %w", err)
	}
	var fresh bool
	if err := tx.QueryRow(ctx, "SELECT $1::timestamptz > clock_timestamp()", entry.ExpiresAt).Scan(&fresh); err != nil {
		return fmt.Errorf("cache postgres check expiry: %w", err)
	}
	if !fresh {
		if _, err := tx.Exec(ctx, `
			DELETE FROM go_cache_entries WHERE namespace_hash = $1 AND key_hash = $2
		`, namespaceHash[:], keyHash[:]); err != nil {
			return fmt.Errorf("cache postgres delete expired set: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("cache postgres commit expired set: %w", err)
		}
		return nil
	}
	generation, err := generationInTx(ctx, tx, namespaceHash[:])
	if err != nil {
		return err
	}
	payload, err := s.encoder.Encode(entry, generation)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO go_cache_entries (namespace_hash, key_hash, payload, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (namespace_hash, key_hash) DO UPDATE
		SET payload = EXCLUDED.payload, expires_at = EXCLUDED.expires_at
	`, namespaceHash[:], keyHash[:], payload, entry.ExpiresAt); err != nil {
		return fmt.Errorf("cache postgres set: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("cache postgres commit set: %w", err)
	}
	return nil
}

func (s *Store[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	namespaceHash := wire.Namespace(namespace)
	keyHash, err := s.encoder.Key(key)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM go_cache_entries WHERE namespace_hash = $1 AND key_hash = $2
	`, namespaceHash[:], keyHash[:]); err != nil {
		return fmt.Errorf("cache postgres invalidate: %w", err)
	}
	return nil
}

func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	namespaceHash := wire.Namespace(namespace)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("cache postgres begin invalidate all: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, rollback(tx))
	}()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryKey(namespaceHash)); err != nil {
		return fmt.Errorf("cache postgres invalidate all lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO go_cache_generations (namespace_hash, generation) VALUES ($1, 1)
		ON CONFLICT (namespace_hash) DO UPDATE
		SET generation = go_cache_generations.generation + 1
	`, namespaceHash[:]); err != nil {
		return fmt.Errorf("cache postgres invalidate all: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("cache postgres commit invalidate all: %w", err)
	}
	return nil
}

// DeleteExpired removes physically retained entries after logical TTL expiry.
func (s *Store[K, V]) DeleteExpired(ctx context.Context) (int64, error) {
	result, err := s.pool.Exec(ctx, "DELETE FROM go_cache_entries WHERE expires_at <= NOW()")
	if err != nil {
		return 0, fmt.Errorf("cache postgres delete expired: %w", err)
	}
	return result.RowsAffected(), nil
}

func advisoryKey(namespaceHash [32]byte) int64 {
	return int64(binary.BigEndian.Uint64(namespaceHash[:8]))
}

func generationInTx(ctx context.Context, tx pgx.Tx, namespaceHash []byte) (uint64, error) {
	var generation int64
	err := tx.QueryRow(ctx, `
		SELECT generation FROM go_cache_generations WHERE namespace_hash = $1
	`, namespaceHash).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cache postgres generation: %w", err)
	}
	if generation < 0 {
		return 0, errors.New("cache postgres: negative generation")
	}
	return uint64(generation), nil
}

func rollback(tx pgx.Tx) error {
	err := tx.Rollback(context.Background())
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}

package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	cache "github.com/justusschock/cache"
	"github.com/justusschock/cache/internal/wire"
)

const invalidationNotificationChannel = "go_cache_invalidation"

// ErrInvalidationPayloadTooLarge reports an invalidation event whose encoded
// payload does not fit in a PostgreSQL NOTIFY payload (8000 bytes).
var ErrInvalidationPayloadTooLarge = errors.New("cache postgres: invalidation payload exceeds PostgreSQL notification limit")

type invalidationConfig[K comparable] struct {
	keyCodec cache.Codec[K]
}

// InvalidationOption configures PostgreSQL invalidation serialization.
type InvalidationOption[K comparable] func(*invalidationConfig[K]) error

// WithInvalidationKeyCodec overrides automatic invalidation-key serialization.
func WithInvalidationKeyCodec[K comparable](codec cache.Codec[K]) InvalidationOption[K] {
	return func(config *invalidationConfig[K]) error {
		if isNil(codec) {
			return ErrNilCodec
		}
		config.keyCodec = codec
		return nil
	}
}

// InvalidationBus distributes invalidations through PostgreSQL LISTEN/NOTIFY.
type InvalidationBus[K comparable] struct {
	pool     *pgxpool.Pool
	keyCodec cache.Codec[K]
}

// NewInvalidationBus constructs a PostgreSQL invalidation bus.
func NewInvalidationBus[K comparable](pool *pgxpool.Pool, options ...InvalidationOption[K]) (*InvalidationBus[K], error) {
	config := invalidationConfig[K]{keyCodec: cache.DefaultCodec[K]()}
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
	return &InvalidationBus[K]{pool: pool, keyCodec: config.keyCodec}, nil
}

// Publish sends event to the subscribers of event.Namespace.
func (b *InvalidationBus[K]) Publish(ctx context.Context, event cache.InvalidationEvent[K]) error {
	encoded, err := wire.EncodeInvalidation(event, b.keyCodec)
	if err != nil {
		return err
	}
	payload := base64.RawStdEncoding.EncodeToString(encoded)
	if len(payload) >= 8000 {
		return ErrInvalidationPayloadTooLarge
	}
	if _, err := b.pool.Exec(ctx, "SELECT pg_notify($1, $2)", invalidationNotificationChannel, payload); err != nil {
		return fmt.Errorf("cache postgres publish invalidation: %w", err)
	}
	return nil
}

// Subscribe calls handler for every event published for namespace until ctx
// ends, returning ctx.Err(), or until receiving or handling an event fails,
// returning that error. A nil handler is rejected with
// cache.ErrNilInvalidationHandler.
func (b *InvalidationBus[K]) Subscribe(ctx context.Context, namespace string, handler cache.InvalidationHandler[K]) error {
	if handler == nil {
		return cache.ErrNilInvalidationHandler
	}
	connection, err := b.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("cache postgres acquire invalidation listener: %w", err)
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, "LISTEN "+invalidationNotificationChannel); err != nil {
		return fmt.Errorf("cache postgres listen invalidation: %w", err)
	}
	defer func() {
		// Subscribe already reports why it returned; an UNLISTEN failure adds nothing actionable.
		_, _ = connection.Exec(context.WithoutCancel(ctx), "UNLISTEN "+invalidationNotificationChannel)
	}()

	for {
		notification, err := connection.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("cache postgres receive invalidation: %w", err)
		}
		encoded, err := base64.RawStdEncoding.DecodeString(notification.Payload)
		if err != nil {
			return fmt.Errorf("cache postgres decode invalidation payload: %w", err)
		}
		event, err := wire.DecodeInvalidation(encoded, b.keyCodec)
		if err != nil {
			return err
		}
		if event.Namespace != namespace {
			continue
		}
		if err := handler(ctx, event); err != nil {
			return err
		}
	}
}

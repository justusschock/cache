//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/postgres"
	"github.com/justusschock/go-cache/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIntegrationContract(t *testing.T) {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@127.0.0.1:5432/cache_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(context.Background()))

	store, err := postgres.New[string, int](pool)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(context.Background()))

	cachetest.RunStoreContract(t, func(*testing.T) cache.Store[string, int] {
		return store
	})
}

func TestLockerIntegration(t *testing.T) {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@127.0.0.1:5432/cache_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store, err := postgres.New[string, int](pool)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(context.Background()))
	locker, err := postgres.NewLocker[string](pool)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	require.NoError(t, err)
	_, err = locker.Lock(context.Background(), t.Name(), "alice", time.Second)
	assert.ErrorIs(t, err, cache.ErrLockNotAcquired)
	require.NoError(t, lease.Refresh(context.Background(), time.Second))
	require.NoError(t, lease.Release(context.Background()))
}

func TestLockerReleaseRejectsNaturalExpiry(t *testing.T) {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@127.0.0.1:5432/cache_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store, err := postgres.New[string, int](pool)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(context.Background()))
	locker, err := postgres.NewLocker[string](pool)
	require.NoError(t, err)

	lease, err := locker.Lock(context.Background(), t.Name(), "alice", 10*time.Millisecond)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	assert.ErrorIs(t, lease.Release(context.Background()), cache.ErrLeaseLost)
}

func TestInvalidationBusIntegration(t *testing.T) {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@127.0.0.1:5432/cache_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(context.Background()))

	cachetest.RunInvalidationBusContract(t, func(t *testing.T) cache.InvalidationBus[string] {
		bus, busErr := postgres.NewInvalidationBus[string](pool)
		require.NoError(t, busErr)
		return bus
	}, cache.InvalidationEvent[string]{Namespace: t.Name(), Kind: cache.InvalidateKey, Key: "alice"})
}

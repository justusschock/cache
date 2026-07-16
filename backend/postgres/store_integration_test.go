//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/postgres"
	"github.com/justusschock/go-cache/internal/cachetest"
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

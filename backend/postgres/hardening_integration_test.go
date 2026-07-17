//go:build integration

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetRechecksExpiryWithServerClockAfterLockWait(t *testing.T) {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@127.0.0.1:5432/cache_test?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store, err := New[string, int](pool)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(ctx))

	namespace := t.Name()
	namespaceHash := wire.Namespace(namespace)
	connection, err := pool.Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(connection.Release)
	_, err = connection.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryKey(namespaceHash))
	require.NoError(t, err)

	entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(200 * time.Millisecond)}
	setResult := make(chan error, 1)
	go func() { setResult <- store.Set(ctx, namespace, "alice", entry) }()
	select {
	case err := <-setResult:
		require.NoError(t, err)
		assert.Fail(t, "set unexpectedly completed while the namespace lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	time.Sleep(200 * time.Millisecond)
	_, err = connection.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryKey(namespaceHash))
	require.NoError(t, err)
	require.NoError(t, <-setResult)

	keyHash, err := store.encoder.Key("alice")
	require.NoError(t, err)
	var count int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM go_cache_entries WHERE namespace_hash = $1 AND key_hash = $2
	`, namespaceHash[:], keyHash[:]).Scan(&count)
	require.NoError(t, err)
	assert.Zero(t, count)
}

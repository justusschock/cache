package disk_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/disk"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreContract(t *testing.T) {
	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, err := disk.Open[string, int](filepath.Join(t.TempDir(), "cache.db"))
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, store.Close())
		})
		return store
	})
}

func TestStorePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute).Round(0)}

	first, err := disk.Open[string, int](path)
	require.NoError(t, err)
	require.NoError(t, first.Set(context.Background(), "users", "alice", entry))
	require.NoError(t, first.Close())

	second, err := disk.Open[string, int](path)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, second.Close())
	})
	got, present, err := second.Get(context.Background(), "users", "alice")
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, entry.Value, got.Value)
	assert.Equal(t, entry.Found, got.Found)
	assert.True(t, entry.ExpiresAt.Equal(got.ExpiresAt))
}

func TestStoreFileIsOwnerOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	store, err := disk.Open[string, int](path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestOpenRejectsEmptyPathAndNilCodecs(t *testing.T) {
	_, err := disk.Open[string, int]("")
	assert.ErrorIs(t, err, disk.ErrEmptyPath)

	_, err = disk.Open("cache.db", disk.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, disk.ErrNilCodec)
	_, err = disk.Open("cache.db", disk.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, disk.ErrNilCodec)
}

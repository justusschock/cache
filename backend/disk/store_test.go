package disk_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/disk"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nilCodec[T any] struct{}

func (*nilCodec[T]) Encode(T) ([]byte, error) { return nil, nil }
func (*nilCodec[T]) Decode([]byte) (T, error) {
	var zero T
	return zero, nil
}

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

	var keyCodec *nilCodec[string]
	_, err = disk.Open(filepath.Join(t.TempDir(), "typed-nil.db"), disk.WithKeyCodec[string, int](keyCodec))
	assert.ErrorIs(t, err, disk.ErrNilCodec)
	var valueCodec *nilCodec[int]
	_, err = disk.Open(filepath.Join(t.TempDir(), "typed-nil.db"), disk.WithValueCodec[string, int](valueCodec))
	assert.ErrorIs(t, err, disk.ErrNilCodec)
}

func openStore(t *testing.T, options ...disk.Option[string, int]) *disk.Store[string, int] {
	t.Helper()
	store, err := disk.Open(filepath.Join(t.TempDir(), "cache.db"), options...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

func TestOrderedStoreContract(t *testing.T) {
	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		return openStore(t, disk.WithOrderedKeys[string, int]())
	})
}

func TestBatchStoreContract(t *testing.T) {
	cachetest.RunBatchStoreContract(t, func(t *testing.T) cache.BatchStore[string, int] {
		return openStore(t)
	})
}

func TestOrderedBatchStoreContract(t *testing.T) {
	cachetest.RunBatchStoreContract(t, func(t *testing.T) cache.BatchStore[string, int] {
		return openStore(t, disk.WithOrderedKeys[string, int]())
	})
}

func TestStoreExpiresEntriesButKeepsNoExpiryEntries(t *testing.T) {
	for name, options := range map[string][]disk.Option[string, int]{
		"hashed":  nil,
		"ordered": {disk.WithOrderedKeys[string, int]()},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := openStore(t, options...)
				ctx := context.Background()
				require.NoError(t, store.SetMany(ctx, "files", []string{"a", "b"}, []cache.Entry[int]{
					{Value: 1, Found: true, ExpiresAt: time.Now().Add(time.Hour)},
					{Value: 2, Found: true},
				}))

				_, present, err := store.GetMany(ctx, "files", []string{"a", "b"})
				require.NoError(t, err)
				assert.Equal(t, []bool{true, true}, present)

				time.Sleep(10 * 365 * 24 * time.Hour)
				entries, present, err := store.GetMany(ctx, "files", []string{"a", "b"})
				require.NoError(t, err)
				assert.Equal(t, []bool{false, true}, present)
				assert.Equal(t, 2, entries[1].Value)
				assert.True(t, entries[1].ExpiresAt.IsZero())
			})
		})
	}
}

func TestStoreNoExpiryPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	first, err := disk.Open[string, int](path)
	require.NoError(t, err)
	require.NoError(t, first.Set(context.Background(), "users", "alice", cache.Entry[int]{Value: 42, Found: true}))
	require.NoError(t, first.Close())

	second, err := disk.Open[string, int](path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
	got, present, err := second.Get(context.Background(), "users", "alice")
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, 42, got.Value)
	assert.True(t, got.ExpiresAt.IsZero())
}

func TestOrderedKeysUseSeparateLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	hashed, err := disk.Open[string, int](path)
	require.NoError(t, err)
	require.NoError(t, hashed.Set(context.Background(), "users", "alice", cache.Entry[int]{Value: 1, Found: true}))
	require.NoError(t, hashed.Close())

	ordered, err := disk.Open(path, disk.WithOrderedKeys[string, int]())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ordered.Close()) })
	_, present, err := ordered.Get(context.Background(), "users", "alice")
	require.NoError(t, err)
	assert.False(t, present)
}

type scanned struct {
	key   string
	value int
}

func scanAll(t *testing.T, store *disk.Store[string, int], namespace string, prefix string) []scanned {
	t.Helper()
	var got []scanned
	require.NoError(t, store.ScanPrefix(context.Background(), namespace, []byte(prefix), func(key string, entry cache.Entry[int]) bool {
		got = append(got, scanned{key: key, value: entry.Value})
		return true
	}))
	return got
}

func TestScanPrefixVisitsLiveEntriesInKeyOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := openStore(t, disk.WithOrderedKeys[string, int]())
		ctx := context.Background()
		keys := []string{"src/b.go", "src/a.go", "src/sub/c.go", "srcx/d.go", "docs/e.md", "src/expiring.go", ""}
		entries := []cache.Entry[int]{
			{Value: 2, Found: true},
			{Value: 1, Found: true},
			{Value: 3, Found: true},
			{Value: 4, Found: true},
			{Value: 5, Found: true},
			{Value: 6, Found: true, ExpiresAt: time.Now().Add(time.Minute)},
			{Value: 7, Found: true},
		}
		require.NoError(t, store.SetMany(ctx, "files", keys, entries))
		require.NoError(t, store.Set(ctx, "other", "src/z.go", cache.Entry[int]{Value: 9, Found: true}))

		assert.Equal(t, []scanned{
			{"src/a.go", 1}, {"src/b.go", 2}, {"src/expiring.go", 6}, {"src/sub/c.go", 3},
		}, scanAll(t, store, "files", "src/"))

		time.Sleep(2 * time.Minute)
		assert.Equal(t, []scanned{
			{"src/a.go", 1}, {"src/b.go", 2}, {"src/sub/c.go", 3},
		}, scanAll(t, store, "files", "src/"))
		assert.Equal(t, []scanned{{"src/sub/c.go", 3}}, scanAll(t, store, "files", "src/sub/"))
		assert.Len(t, scanAll(t, store, "files", ""), 6)
		assert.Equal(t, []scanned{{"", 7}}, scanAll(t, store, "files", "")[:1])
		assert.Empty(t, scanAll(t, store, "files", "zzz"))
		assert.Empty(t, scanAll(t, store, "missing", ""))
	})
}

func TestScanPrefixStopsWhenCallbackReturnsFalse(t *testing.T) {
	store := openStore(t, disk.WithOrderedKeys[string, int]())
	ctx := context.Background()
	require.NoError(t, store.SetMany(ctx, "files", []string{"a1", "a2", "a3"}, []cache.Entry[int]{
		{Value: 1, Found: true}, {Value: 2, Found: true}, {Value: 3, Found: true},
	}))

	var visited []string
	require.NoError(t, store.ScanPrefix(ctx, "files", []byte("a"), func(key string, _ cache.Entry[int]) bool {
		visited = append(visited, key)
		return len(visited) < 2
	}))
	assert.Equal(t, []string{"a1", "a2"}, visited)
}

func TestScanPrefixSkipsEntriesBeforeInvalidateAll(t *testing.T) {
	store := openStore(t, disk.WithOrderedKeys[string, int]())
	ctx := context.Background()
	require.NoError(t, store.Set(ctx, "files", "a", cache.Entry[int]{Value: 1, Found: true}))
	require.NoError(t, store.InvalidateAll(ctx, "files"))
	require.NoError(t, store.Set(ctx, "files", "b", cache.Entry[int]{Value: 2, Found: true}))

	assert.Equal(t, []scanned{{"b", 2}}, scanAll(t, store, "files", ""))
}

func TestDeletePrefixRemovesMatchingRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := openStore(t, disk.WithOrderedKeys[string, int]())
		ctx := context.Background()
		keys := make([]string, 0, 3000)
		entries := make([]cache.Entry[int], 0, 3000)
		for index := range 2500 {
			keys = append(keys, fmt.Sprintf("dir/%05d", index))
			entries = append(entries, cache.Entry[int]{Value: index, Found: true})
		}
		keys = append(keys, "dir/expired", "dirx/keep", "keep")
		entries = append(entries,
			cache.Entry[int]{Value: -1, Found: true, ExpiresAt: time.Now().Add(time.Second)},
			cache.Entry[int]{Value: 1, Found: true},
			cache.Entry[int]{Value: 2, Found: true},
		)
		require.NoError(t, store.SetMany(ctx, "files", keys, entries))
		require.NoError(t, store.Set(ctx, "other", "dir/00001", cache.Entry[int]{Value: 1, Found: true}))
		time.Sleep(time.Minute)

		removed, err := store.DeletePrefix(ctx, "files", []byte("dir/"))
		require.NoError(t, err)
		assert.Equal(t, 2501, removed)
		assert.Equal(t, []scanned{{"dirx/keep", 1}, {"keep", 2}}, scanAll(t, store, "files", ""))
		_, present, err := store.Get(ctx, "other", "dir/00001")
		require.NoError(t, err)
		assert.True(t, present)

		removed, err = store.DeletePrefix(ctx, "files", []byte("dir/"))
		require.NoError(t, err)
		assert.Zero(t, removed)
		removed, err = store.DeletePrefix(ctx, "missing", nil)
		require.NoError(t, err)
		assert.Zero(t, removed)
		removed, err = store.DeletePrefix(ctx, "files", nil)
		require.NoError(t, err)
		assert.Equal(t, 2, removed)
	})
}

func TestPrefixOperationsRequireOrderedKeys(t *testing.T) {
	store := openStore(t)
	err := store.ScanPrefix(context.Background(), "files", nil, func(string, cache.Entry[int]) bool { return true })
	assert.ErrorIs(t, err, disk.ErrOrderedKeysRequired)
	_, err = store.DeletePrefix(context.Background(), "files", nil)
	assert.ErrorIs(t, err, disk.ErrOrderedKeysRequired)
}

func TestPrefixOperationsValidateArguments(t *testing.T) {
	store := openStore(t, disk.WithOrderedKeys[string, int]())
	assert.ErrorIs(t, store.ScanPrefix(context.Background(), "files", nil, nil), disk.ErrNilScanFunc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.ScanPrefix(ctx, "files", nil, func(string, cache.Entry[int]) bool { return true })
	assert.ErrorIs(t, err, context.Canceled)
	_, err = store.DeletePrefix(ctx, "files", nil)
	assert.ErrorIs(t, err, context.Canceled)
}

type fileKey struct {
	Dir  string
	Name string
}

// pathCodec encodes keys as "dir\x00name" so a "dir\x00" prefix selects one directory.
type pathCodec struct{}

func (pathCodec) Encode(key fileKey) ([]byte, error) {
	return []byte(key.Dir + "\x00" + key.Name), nil
}

func (pathCodec) Decode(encoded []byte) (fileKey, error) {
	dir, name, _ := strings.Cut(string(encoded), "\x00")
	return fileKey{Dir: dir, Name: name}, nil
}

func TestScanPrefixWithCustomKeyCodec(t *testing.T) {
	store, err := disk.Open(
		filepath.Join(t.TempDir(), "cache.db"),
		disk.WithOrderedKeys[fileKey, int](),
		disk.WithKeyCodec[fileKey, int](pathCodec{}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	ctx := context.Background()
	require.NoError(t, store.SetMany(ctx, "files", []fileKey{{"a", "x"}, {"a", "y"}, {"ab", "z"}}, []cache.Entry[int]{
		{Value: 1, Found: true}, {Value: 2, Found: true}, {Value: 3, Found: true},
	}))

	var got []fileKey
	require.NoError(t, store.ScanPrefix(ctx, "files", []byte("a\x00"), func(key fileKey, _ cache.Entry[int]) bool {
		got = append(got, key)
		return true
	}))
	assert.Equal(t, []fileKey{{"a", "x"}, {"a", "y"}}, got)
}

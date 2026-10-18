package cachetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StoreFactory returns the store under test for one contract subtest. It may
// return a new store or one shared with other subtests; the suite isolates
// subtests by namespace. Use t.Cleanup to release resources.
type StoreFactory func(t *testing.T) cache.Store[string, int]

// RunStoreContract verifies the behaviour required from every cache.Store:
// hits and misses, negative entries, logical expiration (including an
// expired overwrite of a live entry), namespace isolation, idempotent key
// invalidation, namespace invalidation, cancellation, and entries without
// expiration. Each case runs as a subtest of t.
func RunStoreContract(t *testing.T, factory StoreFactory) {
	t.Helper()

	t.Run("hit and miss", func(t *testing.T) {
		store := scopedStore(t, factory)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))

		got, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.True(t, present)
		assert.Equal(t, entry.Value, got.Value)
		assert.Equal(t, entry.Found, got.Found)
		assert.True(t, entry.ExpiresAt.Equal(got.ExpiresAt))

		got, present, err = store.Get(context.Background(), "users", "missing")
		require.NoError(t, err)
		assert.False(t, present)
		assert.Equal(t, cache.Entry[int]{}, got)
	})

	t.Run("negative entry", func(t *testing.T) {
		store := scopedStore(t, factory)
		entry := cache.Entry[int]{Found: false, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "missing", entry))

		got, present, err := store.Get(context.Background(), "users", "missing")
		require.NoError(t, err)
		assert.True(t, present)
		assert.False(t, got.Found)
	})

	t.Run("logical expiration", func(t *testing.T) {
		store := scopedStore(t, factory)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(5 * time.Millisecond)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		time.Sleep(20 * time.Millisecond)

		got, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
		assert.Equal(t, cache.Entry[int]{}, got)
	})

	t.Run("expired overwrite removes stale value", func(t *testing.T) {
		store := scopedStore(t, factory)
		fresh := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		expired := cache.Entry[int]{Value: 99, Found: true, ExpiresAt: time.Now().Add(-time.Second)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", fresh))
		require.NoError(t, store.Set(context.Background(), "users", "alice", expired))

		got, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
		assert.Equal(t, cache.Entry[int]{}, got)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		store := scopedStore(t, factory)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))

		_, present, err := store.Get(context.Background(), "orders", "alice")
		require.NoError(t, err)
		assert.False(t, present)
	})

	t.Run("key invalidation", func(t *testing.T) {
		store := scopedStore(t, factory)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		require.NoError(t, store.Invalidate(context.Background(), "users", "alice"))

		_, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
	})

	t.Run("invalidate missing is idempotent", func(t *testing.T) {
		store := scopedStore(t, factory)
		require.NoError(t, store.Invalidate(context.Background(), "users", "missing"))
		require.NoError(t, store.Invalidate(context.Background(), "users", "missing"))
	})

	t.Run("namespace invalidation", func(t *testing.T) {
		store := scopedStore(t, factory)
		entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: time.Now().Add(time.Minute)}
		require.NoError(t, store.Set(context.Background(), "users", "alice", entry))
		require.NoError(t, store.Set(context.Background(), "orders", "alice", entry))
		require.NoError(t, store.InvalidateAll(context.Background(), "users"))

		_, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
		_, present, err = store.Get(context.Background(), "orders", "alice")
		require.NoError(t, err)
		assert.True(t, present)
	})

	t.Run("canceled context", func(t *testing.T) {
		store := scopedStore(t, factory)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := store.Get(ctx, "users", "alice")
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, store.Set(ctx, "users", "alice", cache.Entry[int]{}), context.Canceled)
		assert.ErrorIs(t, store.Invalidate(ctx, "users", "alice"), context.Canceled)
		assert.ErrorIs(t, store.InvalidateAll(ctx, "users"), context.Canceled)
	})

	t.Run("no expiry", func(t *testing.T) {
		store := scopedStore(t, factory)
		require.NoError(t, store.Set(context.Background(), "users", "alice", cache.Entry[int]{Value: 42, Found: true}))
		require.NoError(t, store.Set(context.Background(), "users", "missing", cache.Entry[int]{Found: false}))

		got, present, err := store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.True(t, present)
		assert.Equal(t, 42, got.Value)
		assert.True(t, got.Found)
		assert.True(t, got.ExpiresAt.IsZero())

		got, present, err = store.Get(context.Background(), "users", "missing")
		require.NoError(t, err)
		assert.True(t, present)
		assert.False(t, got.Found)
		assert.True(t, got.ExpiresAt.IsZero())

		require.NoError(t, store.InvalidateAll(context.Background(), "users"))
		_, present, err = store.Get(context.Background(), "users", "alice")
		require.NoError(t, err)
		assert.False(t, present)
	})
}

// BatchStoreFactory returns the batch store under test for one contract
// subtest, with the same sharing rules as StoreFactory.
type BatchStoreFactory func(t *testing.T) cache.BatchStore[string, int]

// RunBatchStoreContract verifies the behaviour required from every
// cache.BatchStore: results aligned with keys (including duplicates), batch
// writes visible through Get, expired entries skipped, ErrBatchLength for
// mismatched slices, idempotent batch invalidation, namespace invalidation,
// empty batches, and cancellation. Each case runs as a subtest of t. Run
// RunStoreContract as well; this suite does not repeat its cases.
func RunBatchStoreContract(t *testing.T, factory BatchStoreFactory) {
	t.Helper()

	t.Run("get many aligns results with keys", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx := context.Background()
		require.NoError(t, store.Set(ctx, "users", "alice", cache.Entry[int]{Value: 1, Found: true, ExpiresAt: time.Now().Add(time.Minute)}))
		require.NoError(t, store.Set(ctx, "users", "bob", cache.Entry[int]{Value: 2, Found: true}))
		require.NoError(t, store.Set(ctx, "users", "ghost", cache.Entry[int]{Found: false, ExpiresAt: time.Now().Add(time.Minute)}))

		entries, present, err := store.GetMany(ctx, "users", []string{"bob", "missing", "alice", "ghost", "bob"})
		require.NoError(t, err)
		assert.Equal(t, []bool{true, false, true, true, true}, present)
		require.Len(t, entries, 5)
		assert.Equal(t, 2, entries[0].Value)
		assert.True(t, entries[0].ExpiresAt.IsZero())
		assert.Equal(t, cache.Entry[int]{}, entries[1])
		assert.Equal(t, 1, entries[2].Value)
		assert.False(t, entries[3].Found)
		assert.Equal(t, entries[0], entries[4])
	})

	t.Run("set many round trips through get", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx := context.Background()
		expiresAt := time.Now().Add(time.Minute)
		keys := []string{"alice", "bob", "carol", "alice"}
		entries := []cache.Entry[int]{
			{Value: 1, Found: true, ExpiresAt: expiresAt},
			{Value: 2, Found: true},
			{Found: false, ExpiresAt: expiresAt},
			{Value: 4, Found: true, ExpiresAt: expiresAt},
		}
		require.NoError(t, store.SetMany(ctx, "users", keys, entries))

		got, present, err := store.Get(ctx, "users", "alice")
		require.NoError(t, err)
		assert.True(t, present)
		assert.Equal(t, 4, got.Value)
		assert.True(t, expiresAt.Equal(got.ExpiresAt))
		got, present, err = store.Get(ctx, "users", "bob")
		require.NoError(t, err)
		assert.True(t, present)
		assert.True(t, got.ExpiresAt.IsZero())
		got, present, err = store.Get(ctx, "users", "carol")
		require.NoError(t, err)
		assert.True(t, present)
		assert.False(t, got.Found)
	})

	t.Run("set many skips expired entries", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx := context.Background()
		require.NoError(t, store.Set(ctx, "users", "alice", cache.Entry[int]{Value: 1, Found: true}))
		require.NoError(t, store.SetMany(ctx, "users", []string{"alice", "bob"}, []cache.Entry[int]{
			{Value: 9, Found: true, ExpiresAt: time.Now().Add(-time.Second)},
			{Value: 2, Found: true},
		}))

		_, present, err := store.GetMany(ctx, "users", []string{"alice", "bob"})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, true}, present)
	})

	t.Run("set many rejects mismatched lengths", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		err := store.SetMany(context.Background(), "users", []string{"alice", "bob"}, []cache.Entry[int]{{Value: 1, Found: true}})
		assert.ErrorIs(t, err, cache.ErrBatchLength)
	})

	t.Run("invalidate many", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx := context.Background()
		require.NoError(t, store.SetMany(ctx, "users", []string{"alice", "bob", "carol"}, []cache.Entry[int]{
			{Value: 1, Found: true}, {Value: 2, Found: true}, {Value: 3, Found: true},
		}))
		require.NoError(t, store.InvalidateMany(ctx, "users", []string{"alice", "missing", "carol"}))
		require.NoError(t, store.InvalidateMany(ctx, "users", []string{"alice", "missing", "carol"}))

		_, present, err := store.GetMany(ctx, "users", []string{"alice", "bob", "carol"})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, true, false}, present)
	})

	t.Run("namespace invalidation hides batch entries", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx := context.Background()
		require.NoError(t, store.SetMany(ctx, "users", []string{"alice"}, []cache.Entry[int]{{Value: 1, Found: true}}))
		require.NoError(t, store.SetMany(ctx, "orders", []string{"alice"}, []cache.Entry[int]{{Value: 1, Found: true}}))
		require.NoError(t, store.InvalidateAll(ctx, "users"))

		_, present, err := store.GetMany(ctx, "users", []string{"alice"})
		require.NoError(t, err)
		assert.Equal(t, []bool{false}, present)
		_, present, err = store.GetMany(ctx, "orders", []string{"alice"})
		require.NoError(t, err)
		assert.Equal(t, []bool{true}, present)
	})

	t.Run("empty batches", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx := context.Background()
		entries, present, err := store.GetMany(ctx, "users", nil)
		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.Empty(t, present)
		require.NoError(t, store.SetMany(ctx, "users", nil, nil))
		require.NoError(t, store.InvalidateMany(ctx, "users", nil))
	})

	t.Run("canceled context", func(t *testing.T) {
		store := scopedBatchStore(t, factory)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := store.GetMany(ctx, "users", []string{"alice"})
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, store.SetMany(ctx, "users", []string{"alice"}, []cache.Entry[int]{{Found: true}}), context.Canceled)
		assert.ErrorIs(t, store.InvalidateMany(ctx, "users", []string{"alice"}), context.Canceled)
	})
}

type namespaceBatchStore struct {
	namespaceStore
	batch cache.BatchStore[string, int]
}

func scopedBatchStore(t *testing.T, factory BatchStoreFactory) *namespaceBatchStore {
	t.Helper()
	store := factory(t)
	return &namespaceBatchStore{
		namespaceStore: namespaceStore{
			Store:  store,
			prefix: fmt.Sprintf("%s-%d-", t.Name(), time.Now().UnixNano()),
		},
		batch: store,
	}
}

func (s *namespaceBatchStore) GetMany(ctx context.Context, namespace string, keys []string) ([]cache.Entry[int], []bool, error) {
	return s.batch.GetMany(ctx, s.prefix+namespace, keys)
}

func (s *namespaceBatchStore) SetMany(ctx context.Context, namespace string, keys []string, entries []cache.Entry[int]) error {
	return s.batch.SetMany(ctx, s.prefix+namespace, keys, entries)
}

func (s *namespaceBatchStore) InvalidateMany(ctx context.Context, namespace string, keys []string) error {
	return s.batch.InvalidateMany(ctx, s.prefix+namespace, keys)
}

type namespaceStore struct {
	cache.Store[string, int]
	prefix string
}

func scopedStore(t *testing.T, factory StoreFactory) cache.Store[string, int] {
	t.Helper()
	return &namespaceStore{
		Store:  factory(t),
		prefix: fmt.Sprintf("%s-%d-", t.Name(), time.Now().UnixNano()),
	}
}

func (s *namespaceStore) Get(ctx context.Context, namespace string, key string) (cache.Entry[int], bool, error) {
	return s.Store.Get(ctx, s.prefix+namespace, key)
}

func (s *namespaceStore) Set(ctx context.Context, namespace string, key string, entry cache.Entry[int]) error {
	return s.Store.Set(ctx, s.prefix+namespace, key, entry)
}

func (s *namespaceStore) Invalidate(ctx context.Context, namespace string, key string) error {
	return s.Store.Invalidate(ctx, s.prefix+namespace, key)
}

func (s *namespaceStore) InvalidateAll(ctx context.Context, namespace string) error {
	return s.Store.InvalidateAll(ctx, s.prefix+namespace)
}

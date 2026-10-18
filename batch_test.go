package cache_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ cache.BatchCache[string, int] = (*cache.StandardCache[string, int])(nil)
var _ cache.BatchStore[string, int] = (*memory.Store[string, int])(nil)

// countingBatchStore records which store methods a cache uses.
type countingBatchStore struct {
	*recordingStore[string, int]
	getMany        int
	setMany        int
	invalidateMany int
	err            error
}

func newCountingBatchStore() *countingBatchStore {
	return &countingBatchStore{recordingStore: newRecordingStore[string, int]()}
}

func (s *countingBatchStore) GetMany(ctx context.Context, namespace string, keys []string) ([]cache.Entry[int], []bool, error) {
	s.getMany++
	if s.err != nil {
		return nil, nil, s.err
	}
	entries := make([]cache.Entry[int], len(keys))
	present := make([]bool, len(keys))
	for index, key := range keys {
		entries[index], present[index], _ = s.Get(ctx, namespace, key)
	}
	return entries, present, nil
}

func (s *countingBatchStore) SetMany(ctx context.Context, namespace string, keys []string, entries []cache.Entry[int]) error {
	s.setMany++
	if s.err != nil {
		return s.err
	}
	for index, key := range keys {
		if err := s.Set(ctx, namespace, key, entries[index]); err != nil {
			return err
		}
	}
	return nil
}

func (s *countingBatchStore) InvalidateMany(ctx context.Context, namespace string, keys []string) error {
	s.invalidateMany++
	if s.err != nil {
		return s.err
	}
	for _, key := range keys {
		if err := s.Invalidate(ctx, namespace, key); err != nil {
			return err
		}
	}
	return nil
}

// shortBatchStore violates the BatchStore alignment contract.
type shortBatchStore struct {
	*countingBatchStore
}

func (shortBatchStore) GetMany(context.Context, string, []string) ([]cache.Entry[int], []bool, error) {
	return nil, nil, nil
}

// FR-CACHE-03
func TestNewAcceptsNoExpiryButRejectsOtherNonPositiveTTLs(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, int]()
	_, err := cache.New("users", cache.NoExpiry, store)
	require.NoError(t, err)

	for _, ttl := range []time.Duration{0, -1, -time.Hour, cache.NoExpiry + 1} {
		_, err := cache.New("users", ttl, store)
		assert.ErrorIs(t, err, cache.ErrInvalidTTL, ttl)
	}
}

// FR-CACHE-03
func TestNoExpiryEntriesNeverExpire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newRecordingStore[string, int]()
		c, err := cache.New("users", cache.NoExpiry, store)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, c.Set(ctx, "forever", 1))
		assert.True(t, store.lastSet.ExpiresAt.IsZero())
		assert.True(t, store.lastSet.NeverExpires())
		require.NoError(t, c.SetWithTTL(ctx, "brief", 2, time.Minute))
		assert.False(t, store.lastSet.NeverExpires())

		time.Sleep(100 * 365 * 24 * time.Hour)

		value, found, err := c.Get(ctx, "forever")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 1, value)
		_, found, err = c.Get(ctx, "brief")
		require.NoError(t, err)
		assert.False(t, found)
	})
}

// FR-CACHE-03
func TestSetWithTTLAcceptsNoExpiryOnExpiringCache(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newRecordingStore[string, int]()
		c, err := cache.New("users", time.Minute, store)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, c.Set(ctx, "default", 1))
		require.NoError(t, c.SetWithTTL(ctx, "forever", 2, cache.NoExpiry))
		assert.ErrorIs(t, c.SetWithTTL(ctx, "invalid", 3, -time.Second), cache.ErrInvalidTTL)

		time.Sleep(time.Hour)
		_, found, err := c.Get(ctx, "default")
		require.NoError(t, err)
		assert.False(t, found)
		entry, present, err := c.GetEntry(ctx, "forever")
		require.NoError(t, err)
		assert.True(t, present)
		assert.True(t, entry.ExpiresAt.IsZero())
	})
}

// FR-CACHE-03, FR-CACHE-04
func TestSetEntryAcceptsNoExpiryAndRejectsExpiredEntries(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, int]()
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, c.SetEntry(ctx, "forever", cache.Entry[int]{Value: 1, Found: true}))
	entry, present, err := c.GetEntry(ctx, "forever")
	require.NoError(t, err)
	assert.True(t, present)
	require.NoError(t, c.SetEntry(ctx, "copy", entry))

	expired := cache.Entry[int]{Value: 1, Found: true, ExpiresAt: time.Now().Add(-time.Second)}
	assert.ErrorIs(t, c.SetEntry(ctx, "expired", expired), cache.ErrInvalidTTL)
}

// FR-CACHE-03
func TestEntryExpiration(t *testing.T) {
	t.Parallel()

	now := time.Now()
	assert.False(t, cache.Entry[int]{}.Expired(now))
	assert.True(t, cache.Entry[int]{}.NeverExpires())
	assert.True(t, cache.Entry[int]{ExpiresAt: now}.Expired(now))
	assert.False(t, cache.Entry[int]{ExpiresAt: now.Add(time.Nanosecond)}.Expired(now))
	assert.False(t, cache.Entry[int]{ExpiresAt: now}.NeverExpires())
}

// FR-CACHE-05
func TestBatchOperationsUseBatchStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newCountingBatchStore()
		c, err := cache.New[string, int]("users", time.Minute, store)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, c.SetMany(ctx, []string{"alice", "bob"}, []int{1, 2}))
		require.NoError(t, c.SetManyWithTTL(ctx, []string{"carol"}, []int{3}, cache.NoExpiry))
		store.entries["ghost"] = cache.Entry[int]{Found: false}
		assert.Equal(t, 2, store.setMany)
		assert.True(t, store.entries["carol"].NeverExpires())
		assert.True(t, store.entries["alice"].ExpiresAt.Equal(time.Now().Add(time.Minute)))

		values, found, err := c.GetMany(ctx, []string{"carol", "missing", "alice", "ghost"})
		require.NoError(t, err)
		assert.Equal(t, []int{3, 0, 1, 0}, values)
		assert.Equal(t, []bool{true, false, true, false}, found)
		assert.Equal(t, 1, store.getMany)

		time.Sleep(2 * time.Minute)
		_, found, err = c.GetMany(ctx, []string{"alice", "bob", "carol"})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, false, true}, found)

		require.NoError(t, c.InvalidateMany(ctx, []string{"carol", "missing"}))
		assert.Equal(t, 1, store.invalidateMany)
		assert.NotContains(t, store.entries, "carol")
	})
}

// FR-CACHE-05
func TestBatchOperationsFallBackToStoreLoop(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, int]()
	c, err := cache.New[string, int]("users", cache.NoExpiry, store)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, c.SetMany(ctx, []string{"alice", "bob", "alice"}, []int{1, 2, 3}))
	assert.Len(t, store.entries, 2)
	assert.True(t, store.entries["alice"].NeverExpires())

	values, found, err := c.GetMany(ctx, []string{"alice", "missing", "bob"})
	require.NoError(t, err)
	assert.Equal(t, []int{3, 0, 2}, values)
	assert.Equal(t, []bool{true, false, true}, found)

	require.NoError(t, c.InvalidateMany(ctx, []string{"alice", "missing"}))
	assert.Len(t, store.entries, 1)

	store.setErr = errors.New("write failed")
	assert.ErrorContains(t, c.SetMany(ctx, []string{"carol"}, []int{4}), "write failed")
}

// FR-CACHE-05
func TestBatchOperationsValidateArguments(t *testing.T) {
	t.Parallel()

	store := newCountingBatchStore()
	c, err := cache.New[string, int]("users", time.Minute, store)
	require.NoError(t, err)
	ctx := context.Background()

	assert.ErrorIs(t, c.SetMany(ctx, []string{"alice"}, nil), cache.ErrBatchLength)
	assert.ErrorIs(t, c.SetManyWithTTL(ctx, []string{"alice"}, []int{1}, 0), cache.ErrInvalidTTL)

	values, found, err := c.GetMany(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, values)
	assert.Empty(t, found)
	require.NoError(t, c.SetMany(ctx, nil, nil))
	require.NoError(t, c.InvalidateMany(ctx, nil))
	assert.Zero(t, store.getMany+store.setMany+store.invalidateMany)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = c.GetMany(canceled, []string{"alice"})
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, c.SetMany(canceled, []string{"alice"}, []int{1}), context.Canceled)
	assert.ErrorIs(t, c.InvalidateMany(canceled, []string{"alice"}), context.Canceled)
}

// FR-CACHE-05, FR-CACHE-22
func TestBatchOperationsWrapStoreErrors(t *testing.T) {
	t.Parallel()

	store := newCountingBatchStore()
	store.err = errors.New("batch failed")
	c, err := cache.New[string, int]("users", time.Minute, store)
	require.NoError(t, err)
	ctx := context.Background()

	_, _, err = c.GetMany(ctx, []string{"alice"})
	assert.ErrorIs(t, err, store.err)
	assert.ErrorIs(t, c.SetMany(ctx, []string{"alice"}, []int{1}), store.err)
	assert.ErrorIs(t, c.InvalidateMany(ctx, []string{"alice"}), store.err)
}

// FR-CACHE-05
func TestGetManyRejectsMisalignedBatchStoreResults(t *testing.T) {
	t.Parallel()

	c, err := cache.New[string, int]("users", time.Minute, shortBatchStore{newCountingBatchStore()})
	require.NoError(t, err)
	_, _, err = c.GetMany(context.Background(), []string{"alice"})
	assert.ErrorIs(t, err, cache.ErrBatchLength)
}

// FR-CACHE-05
func TestBatchCacheWithMemoryStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := memory.New[string, int]()
		t.Cleanup(func() { assert.NoError(t, store.Close()) })
		c, err := cache.New[string, int]("users", cache.NoExpiry, store)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, c.SetMany(ctx, []string{"alice", "bob"}, []int{1, 2}))
		time.Sleep(24 * time.Hour)
		values, found, err := c.GetMany(ctx, []string{"alice", "bob"})
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, values)
		assert.Equal(t, []bool{true, true}, found)
	})
}

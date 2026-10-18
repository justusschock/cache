package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingStore[K comparable, V any] struct {
	mu      sync.Mutex
	entries map[K]cache.Entry[V]
	lastSet cache.Entry[V]
	setErr  error
}

func newRecordingStore[K comparable, V any]() *recordingStore[K, V] {
	return &recordingStore[K, V]{entries: make(map[K]cache.Entry[V])}
}

func (s *recordingStore[K, V]) Get(_ context.Context, _ string, key K) (cache.Entry[V], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	return entry, ok, nil
}

func (s *recordingStore[K, V]) Set(_ context.Context, _ string, key K, entry cache.Entry[V]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSet = entry
	if s.setErr != nil {
		return s.setErr
	}
	s.entries[key] = entry
	return nil
}

func (s *recordingStore[K, V]) Invalidate(_ context.Context, _ string, key K) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

func (s *recordingStore[K, V]) InvalidateAll(_ context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.entries)
	return nil
}

// FR-CACHE-01, FR-CACHE-22
func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, string]()

	_, err := cache.New("", time.Minute, store)
	assert.ErrorIs(t, err, cache.ErrEmptyNamespace)

	_, err = cache.New("users", 0, store)
	assert.ErrorIs(t, err, cache.ErrInvalidTTL)

	_, err = cache.New[string, string]("users", time.Minute, nil)
	assert.ErrorIs(t, err, cache.ErrNilStore)
}

// FR-CACHE-01
func TestCacheSetUsesDefaultTTLAndGetReturnsValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newRecordingStore[string, int]()
		c, err := cache.New("users", time.Minute, store)
		require.NoError(t, err)

		require.NoError(t, c.Set(context.Background(), "alice", 42))
		assert.True(t, store.lastSet.Found)
		assert.Equal(t, 42, store.lastSet.Value)
		assert.True(t, store.lastSet.ExpiresAt.Equal(time.Now().Add(time.Minute)))

		value, found, err := c.Get(context.Background(), "alice")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 42, value)

		time.Sleep(time.Minute)
		_, found, err = c.Get(context.Background(), "alice")
		require.NoError(t, err)
		assert.False(t, found, "entry must expire exactly at its TTL")
	})
}

// FR-CACHE-01
func TestCacheSetWithTTLOverridesDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newRecordingStore[string, string]()
		c, err := cache.New("users", time.Hour, store)
		require.NoError(t, err)

		require.NoError(t, c.SetWithTTL(context.Background(), "alice", "value", 25*time.Millisecond))
		assert.True(t, store.lastSet.ExpiresAt.Equal(time.Now().Add(25*time.Millisecond)))
		assert.ErrorIs(t, c.SetWithTTL(context.Background(), "alice", "value", 0), cache.ErrInvalidTTL)

		time.Sleep(24 * time.Millisecond)
		_, found, err := c.Get(context.Background(), "alice")
		require.NoError(t, err)
		assert.True(t, found)
		time.Sleep(time.Millisecond)
		_, found, err = c.Get(context.Background(), "alice")
		require.NoError(t, err)
		assert.False(t, found)
	})
}

// FR-CACHE-02
func TestCacheDistinguishesCachedZeroAndMiss(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, int]()
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)
	require.NoError(t, c.Set(context.Background(), "zero", 0))

	value, found, err := c.Get(context.Background(), "zero")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Zero(t, value)

	value, found, err = c.Get(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, value)
}

// FR-CACHE-02
func TestCacheDoesNotExposeExpiredOrNegativeEntries(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, string]()
	store.entries["expired"] = cache.Entry[string]{Value: "stale", Found: true, ExpiresAt: time.Now().Add(-time.Second)}
	store.entries["negative"] = cache.Entry[string]{Found: false, ExpiresAt: time.Now().Add(time.Minute)}
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)

	value, found, err := c.Get(context.Background(), "expired")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, value)

	value, found, err = c.Get(context.Background(), "negative")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, value)
}

// FR-CACHE-22
func TestCachePropagatesStoreErrors(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, string]()
	store.setErr = errors.New("write failed")
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)

	err = c.Set(context.Background(), "alice", "value")
	assert.ErrorContains(t, err, "write failed")
}

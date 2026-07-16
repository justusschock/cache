package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
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

func TestCacheSetUsesDefaultTTLAndGetReturnsValue(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, int]()
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)

	before := time.Now().Add(time.Minute)
	require.NoError(t, c.Set(context.Background(), "alice", 42))
	after := time.Now().Add(time.Minute)

	assert.True(t, store.lastSet.Found)
	assert.Equal(t, 42, store.lastSet.Value)
	assert.False(t, store.lastSet.ExpiresAt.Before(before))
	assert.False(t, store.lastSet.ExpiresAt.After(after))

	value, found, err := c.Get(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 42, value)
}

func TestCacheSetWithTTLOverridesDefault(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, string]()
	c, err := cache.New("users", time.Hour, store)
	require.NoError(t, err)

	before := time.Now().Add(25 * time.Millisecond)
	require.NoError(t, c.SetWithTTL(context.Background(), "alice", "value", 25*time.Millisecond))
	after := time.Now().Add(25 * time.Millisecond)

	assert.False(t, store.lastSet.ExpiresAt.Before(before))
	assert.False(t, store.lastSet.ExpiresAt.After(after))
	assert.ErrorIs(t, c.SetWithTTL(context.Background(), "alice", "value", 0), cache.ErrInvalidTTL)
}

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

func TestCachePropagatesStoreErrors(t *testing.T) {
	t.Parallel()

	store := newRecordingStore[string, string]()
	store.setErr = errors.New("write failed")
	c, err := cache.New("users", time.Minute, store)
	require.NoError(t, err)

	err = c.Set(context.Background(), "alice", "value")
	assert.ErrorContains(t, err, "write failed")
}

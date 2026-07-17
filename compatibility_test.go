package cache_test

import (
	"context"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/require"
)

type compatibilityStore struct{}

func (compatibilityStore) Get(context.Context, string, string) (cache.Entry[int], bool, error) {
	return cache.Entry[int]{}, false, nil
}

func (compatibilityStore) Set(context.Context, string, string, cache.Entry[int]) error {
	return nil
}

func (compatibilityStore) Invalidate(context.Context, string, string) error {
	return nil
}

func (compatibilityStore) InvalidateAll(context.Context, string) error {
	return nil
}

var _ cache.Store[string, int] = compatibilityStore{}
var _ cache.Cache[string, int] = (*cache.StandardCache[string, int])(nil)
var _ cache.EntryCache[string, int] = (*cache.StandardCache[string, int])(nil)

func TestExistingConstructorsRemainCallable(t *testing.T) {
	standard, err := cache.New("compat", time.Minute, compatibilityStore{})
	require.NoError(t, err)

	_, err = cache.NewLayered([]cache.Store[string, int]{compatibilityStore{}})
	require.NoError(t, err)

	_, err = cache.NewPullThrough(
		"compat",
		standard,
		func(context.Context, string) (int, bool, error) { return 1, true, nil },
	)
	require.NoError(t, err)
}

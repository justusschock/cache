package memcached_test

import (
	"testing"

	"github.com/justusschock/go-cache/backend/memcached"
	"github.com/stretchr/testify/assert"
)

type nilCodec[T any] struct{}

func (*nilCodec[T]) Encode(T) ([]byte, error) { return nil, nil }
func (*nilCodec[T]) Decode([]byte) (T, error) {
	var zero T
	return zero, nil
}

// FR-CACHE-22
func TestNewRejectsNilClientAndCodecs(t *testing.T) {
	_, err := memcached.New[string, int](nil)
	assert.ErrorIs(t, err, memcached.ErrNilClient)
	_, err = memcached.New[string, int](nil, memcached.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
	_, err = memcached.New[string, int](nil, memcached.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
	var keyCodec *nilCodec[string]
	_, err = memcached.New[string, int](nil, memcached.WithKeyCodec[string, int](keyCodec))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
	var valueCodec *nilCodec[int]
	_, err = memcached.New[string, int](nil, memcached.WithValueCodec[string, int](valueCodec))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
}

// FR-CACHE-22
func TestNewLockerRejectsNilClient(t *testing.T) {
	_, err := memcached.NewLocker[string](nil)
	assert.ErrorIs(t, err, memcached.ErrNilClient)
	var codec *nilCodec[string]
	_, err = memcached.NewLocker(nil, memcached.WithLockerKeyCodec[string](codec))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
}

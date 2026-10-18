package redis_test

import (
	"testing"

	cache "github.com/justusschock/cache"
	rediscache "github.com/justusschock/cache/backend/redis"
	goredis "github.com/redis/go-redis/v9"
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
	_, err := rediscache.New[string, int](nil)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)

	_, err = rediscache.New[string, int](nil, rediscache.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
	_, err = rediscache.New[string, int](nil, rediscache.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
	var client *goredis.Client
	_, err = rediscache.New[string, int](client)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	var keyCodec *nilCodec[string]
	_, err = rediscache.New[string, int](nil, rediscache.WithKeyCodec[string, int](keyCodec))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
	var valueCodec *nilCodec[int]
	_, err = rediscache.New[string, int](nil, rediscache.WithValueCodec[string, int](valueCodec))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
}

// FR-CACHE-22
func TestNewLockerRejectsNilClientAndCodec(t *testing.T) {
	_, err := rediscache.NewLocker[string](nil)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	_, err = rediscache.NewLocker(nil, rediscache.WithLockerKeyCodec[string](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
	var client *goredis.Client
	_, err = rediscache.NewLocker[string](client)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	var codec *nilCodec[string]
	_, err = rediscache.NewLocker(nil, rediscache.WithLockerKeyCodec[string](cache.Codec[string](codec)))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
}

// FR-CACHE-22
func TestNewInvalidationBusRejectsNilClientAndCodec(t *testing.T) {
	_, err := rediscache.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	_, err = rediscache.NewInvalidationBus(nil, rediscache.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
	var client *goredis.Client
	_, err = rediscache.NewInvalidationBus[string](client)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	var codec *nilCodec[string]
	_, err = rediscache.NewInvalidationBus(nil, rediscache.WithInvalidationKeyCodec[string](codec))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
}

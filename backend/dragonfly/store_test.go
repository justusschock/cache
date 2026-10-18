package dragonfly_test

import (
	"testing"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/dragonfly"
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
func TestNewRejectsNilClient(t *testing.T) {
	_, err := dragonfly.New[string, int](nil)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
	var client *goredis.Client
	_, err = dragonfly.New[string, int](client)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
	var codec *nilCodec[string]
	_, err = dragonfly.New[string, int](nil, dragonfly.WithKeyCodec[string, int](cache.Codec[string](codec)))
	assert.ErrorIs(t, err, dragonfly.ErrNilCodec)
}

// FR-CACHE-22
func TestNewLockerRejectsNilClient(t *testing.T) {
	_, err := dragonfly.NewLocker[string](nil)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
	var client *goredis.Client
	_, err = dragonfly.NewLocker[string](client)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
	var codec *nilCodec[string]
	_, err = dragonfly.NewLocker(nil, dragonfly.WithLockerKeyCodec[string](codec))
	assert.ErrorIs(t, err, dragonfly.ErrNilCodec)
}

// FR-CACHE-22
func TestNewInvalidationBusRejectsNilClient(t *testing.T) {
	_, err := dragonfly.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
	var client *goredis.Client
	_, err = dragonfly.NewInvalidationBus[string](client)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
	var codec *nilCodec[string]
	_, err = dragonfly.NewInvalidationBus(nil, dragonfly.WithInvalidationKeyCodec[string](codec))
	assert.ErrorIs(t, err, dragonfly.ErrNilCodec)
}

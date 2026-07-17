package hazelcast_test

import (
	"context"
	"testing"

	hazelcastcache "github.com/justusschock/go-cache/backend/hazelcast"
	"github.com/stretchr/testify/assert"
)

type nilCodec[T any] struct{}

func (*nilCodec[T]) Encode(T) ([]byte, error) { return nil, nil }
func (*nilCodec[T]) Decode([]byte) (T, error) {
	var zero T
	return zero, nil
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	_, err := hazelcastcache.New[string, int](context.Background(), nil)
	assert.ErrorIs(t, err, hazelcastcache.ErrNilClient)
	_, err = hazelcastcache.New[string, int](context.Background(), nil, hazelcastcache.WithMapName[string, int](""))
	assert.ErrorIs(t, err, hazelcastcache.ErrEmptyMapName)
	_, err = hazelcastcache.New[string, int](context.Background(), nil, hazelcastcache.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, hazelcastcache.ErrNilCodec)
	var keyCodec *nilCodec[string]
	_, err = hazelcastcache.New[string, int](context.Background(), nil, hazelcastcache.WithKeyCodec[string, int](keyCodec))
	assert.ErrorIs(t, err, hazelcastcache.ErrNilCodec)
	var valueCodec *nilCodec[int]
	_, err = hazelcastcache.New[string, int](context.Background(), nil, hazelcastcache.WithValueCodec[string, int](valueCodec))
	assert.ErrorIs(t, err, hazelcastcache.ErrNilCodec)
}

func TestNewLockerRejectsNilClient(t *testing.T) {
	_, err := hazelcastcache.NewLocker[string](context.Background(), nil)
	assert.ErrorIs(t, err, hazelcastcache.ErrNilClient)
}

func TestNewInvalidationBusRejectsNilClientAndCodec(t *testing.T) {
	_, err := hazelcastcache.NewInvalidationBus[string](context.Background(), nil)
	assert.ErrorIs(t, err, hazelcastcache.ErrNilClient)
	_, err = hazelcastcache.NewInvalidationBus(context.Background(), nil, hazelcastcache.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, hazelcastcache.ErrNilCodec)
	var codec *nilCodec[string]
	_, err = hazelcastcache.NewInvalidationBus(context.Background(), nil, hazelcastcache.WithInvalidationKeyCodec[string](codec))
	assert.ErrorIs(t, err, hazelcastcache.ErrNilCodec)
}

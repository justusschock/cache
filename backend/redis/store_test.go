package redis_test

import (
	"testing"

	rediscache "github.com/justusschock/go-cache/backend/redis"
	"github.com/stretchr/testify/assert"
)

func TestNewRejectsNilClientAndCodecs(t *testing.T) {
	_, err := rediscache.New[string, int](nil)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)

	_, err = rediscache.New[string, int](nil, rediscache.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
	_, err = rediscache.New[string, int](nil, rediscache.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
}

func TestNewLockerRejectsNilClientAndCodec(t *testing.T) {
	_, err := rediscache.NewLocker[string](nil)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	_, err = rediscache.NewLocker(nil, rediscache.WithLockerKeyCodec[string](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
}

func TestNewInvalidationBusRejectsNilClientAndCodec(t *testing.T) {
	_, err := rediscache.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, rediscache.ErrNilClient)
	_, err = rediscache.NewInvalidationBus(nil, rediscache.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, rediscache.ErrNilCodec)
}

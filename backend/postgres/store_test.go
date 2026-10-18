package postgres_test

import (
	"testing"

	"github.com/justusschock/go-cache/backend/postgres"
	"github.com/stretchr/testify/assert"
)

type nilCodec[T any] struct{}

func (*nilCodec[T]) Encode(T) ([]byte, error) { return nil, nil }
func (*nilCodec[T]) Decode([]byte) (T, error) {
	var zero T
	return zero, nil
}

// FR-CACHE-22
func TestNewRejectsNilPoolAndCodecs(t *testing.T) {
	_, err := postgres.New[string, int](nil)
	assert.ErrorIs(t, err, postgres.ErrNilPool)
	_, err = postgres.New[string, int](nil, postgres.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
	_, err = postgres.New[string, int](nil, postgres.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
	var keyCodec *nilCodec[string]
	_, err = postgres.New[string, int](nil, postgres.WithKeyCodec[string, int](keyCodec))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
	var valueCodec *nilCodec[int]
	_, err = postgres.New[string, int](nil, postgres.WithValueCodec[string, int](valueCodec))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
}

// FR-CACHE-22
func TestNewLockerRejectsNilPool(t *testing.T) {
	_, err := postgres.NewLocker[string](nil)
	assert.ErrorIs(t, err, postgres.ErrNilPool)
	var codec *nilCodec[string]
	_, err = postgres.NewLocker(nil, postgres.WithLockerKeyCodec[string](codec))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
}

// FR-CACHE-22
func TestNewInvalidationBusRejectsNilPoolAndCodec(t *testing.T) {
	_, err := postgres.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, postgres.ErrNilPool)
	_, err = postgres.NewInvalidationBus(nil, postgres.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
	var codec *nilCodec[string]
	_, err = postgres.NewInvalidationBus(nil, postgres.WithInvalidationKeyCodec[string](codec))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
}

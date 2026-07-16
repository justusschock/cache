package postgres_test

import (
	"testing"

	"github.com/justusschock/go-cache/backend/postgres"
	"github.com/stretchr/testify/assert"
)

func TestNewRejectsNilPoolAndCodecs(t *testing.T) {
	_, err := postgres.New[string, int](nil)
	assert.ErrorIs(t, err, postgres.ErrNilPool)
	_, err = postgres.New[string, int](nil, postgres.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
	_, err = postgres.New[string, int](nil, postgres.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
}

func TestNewLockerRejectsNilPool(t *testing.T) {
	_, err := postgres.NewLocker[string](nil)
	assert.ErrorIs(t, err, postgres.ErrNilPool)
}

func TestNewInvalidationBusRejectsNilPoolAndCodec(t *testing.T) {
	_, err := postgres.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, postgres.ErrNilPool)
	_, err = postgres.NewInvalidationBus(nil, postgres.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, postgres.ErrNilCodec)
}

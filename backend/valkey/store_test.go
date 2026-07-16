package valkey_test

import (
	"testing"

	valkeycache "github.com/justusschock/go-cache/backend/valkey"
	"github.com/stretchr/testify/assert"
)

func TestNewRejectsNilClientAndCodecs(t *testing.T) {
	_, err := valkeycache.New[string, int](nil)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	_, err = valkeycache.New[string, int](nil, valkeycache.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
	_, err = valkeycache.New[string, int](nil, valkeycache.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
}

func TestNewLockerRejectsNilClient(t *testing.T) {
	_, err := valkeycache.NewLocker[string](nil)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
}

func TestNewInvalidationBusRejectsNilClientAndCodec(t *testing.T) {
	_, err := valkeycache.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	_, err = valkeycache.NewInvalidationBus(nil, valkeycache.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
}

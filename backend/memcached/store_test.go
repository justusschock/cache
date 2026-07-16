package memcached_test

import (
	"testing"

	"github.com/justusschock/go-cache/backend/memcached"
	"github.com/stretchr/testify/assert"
)

func TestNewRejectsNilClientAndCodecs(t *testing.T) {
	_, err := memcached.New[string, int](nil)
	assert.ErrorIs(t, err, memcached.ErrNilClient)
	_, err = memcached.New[string, int](nil, memcached.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
	_, err = memcached.New[string, int](nil, memcached.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, memcached.ErrNilCodec)
}

func TestNewLockerRejectsNilClient(t *testing.T) {
	_, err := memcached.NewLocker[string](nil)
	assert.ErrorIs(t, err, memcached.ErrNilClient)
}

package hazelcast_test

import (
	"context"
	"testing"

	hazelcastcache "github.com/justusschock/go-cache/backend/hazelcast"
	"github.com/stretchr/testify/assert"
)

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	_, err := hazelcastcache.New[string, int](context.Background(), nil)
	assert.ErrorIs(t, err, hazelcastcache.ErrNilClient)
	_, err = hazelcastcache.New[string, int](context.Background(), nil, hazelcastcache.WithMapName[string, int](""))
	assert.ErrorIs(t, err, hazelcastcache.ErrEmptyMapName)
	_, err = hazelcastcache.New[string, int](context.Background(), nil, hazelcastcache.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, hazelcastcache.ErrNilCodec)
}

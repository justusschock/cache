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

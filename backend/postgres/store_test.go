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

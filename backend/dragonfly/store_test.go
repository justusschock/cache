package dragonfly_test

import (
	"testing"

	"github.com/justusschock/go-cache/backend/dragonfly"
	"github.com/stretchr/testify/assert"
)

func TestNewRejectsNilClient(t *testing.T) {
	_, err := dragonfly.New[string, int](nil)
	assert.ErrorIs(t, err, dragonfly.ErrNilClient)
}

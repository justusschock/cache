package hazelcast

import (
	"context"
	"testing"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
)

func TestSubscribeRejectsNilHandler(t *testing.T) {
	bus := &InvalidationBus[string]{}
	var err error
	assert.NotPanics(t, func() {
		err = bus.Subscribe(context.Background(), "users", nil)
	})
	assert.ErrorIs(t, err, cache.ErrNilInvalidationHandler)
}

package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlightSharesTypedResultAndLetsWaiterCancel(t *testing.T) {
	var group flightGroup[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	loader := func(context.Context) (int, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return 42, true, nil
	}

	owner := make(chan int, 1)
	go func() {
		value, _, _ := group.Do(context.Background(), "key", loader)
		owner <- value
	}()
	<-started

	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := group.Do(waiterCtx, "key", loader)
	require.ErrorIs(t, err, context.Canceled)

	close(release)
	assert.Equal(t, 42, <-owner)
	assert.Equal(t, int32(1), calls.Load())
}

func TestFlightPreservesLoaderError(t *testing.T) {
	var group flightGroup[string, int]
	wantErr := errors.New("load failed")

	_, found, err := group.Do(context.Background(), "key", func(context.Context) (int, bool, error) {
		return 0, false, wantErr
	})

	assert.False(t, found)
	assert.ErrorIs(t, err, wantErr)
}

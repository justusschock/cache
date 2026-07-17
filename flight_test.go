package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

type flightOutcome[V any] struct {
	value V
	found bool
	err   error
}

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

func TestFlightCancelsFillOnlyAfterLastWaiterDeparts(t *testing.T) {
	var group flightGroup[string, int]
	started := make(chan struct{})
	canceled := make(chan struct{})
	var calls atomic.Int32
	loader := func(ctx context.Context) (int, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			close(canceled)
			return 0, false, ctx.Err()
		}
		return 42, true, nil
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan flightOutcome[int], 1)
	go func() {
		value, found, err := group.Do(firstCtx, "key", loader)
		firstResult <- flightOutcome[int]{value: value, found: found, err: err}
	}()
	<-started

	secondBaseCtx, cancelSecond := context.WithCancel(context.Background())
	secondCtx := &observedDoneContext{Context: secondBaseCtx, observed: make(chan struct{})}
	secondResult := make(chan flightOutcome[int], 1)
	go func() {
		value, found, err := group.Do(secondCtx, "key", loader)
		secondResult <- flightOutcome[int]{value: value, found: found, err: err}
	}()
	<-secondCtx.observed

	cancelFirst()
	assert.ErrorIs(t, (<-firstResult).err, context.Canceled)
	select {
	case <-canceled:
		assert.Fail(t, "fill canceled while another waiter was active")
	default:
	}

	cancelSecond()
	assert.ErrorIs(t, (<-secondResult).err, context.Canceled)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		assert.Fail(t, "fill context remained active after its last waiter departed")
		return
	}

	value, found, err := group.Do(context.Background(), "key", loader)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 42, value)
	assert.Equal(t, int32(2), calls.Load())
}

func TestFlightCompletionAndDepartureAlwaysStartLaterLifecycleFresh(t *testing.T) {
	for iteration := range 100 {
		var group flightGroup[string, int]
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		loader := func(context.Context) (int, bool, error) {
			call := calls.Add(1)
			if call == 1 {
				close(started)
				<-release
			}
			return int(call), true, nil
		}

		firstCtx, cancelFirst := context.WithCancel(context.Background())
		firstResult := make(chan flightOutcome[int], 1)
		go func() {
			value, found, err := group.Do(firstCtx, "key", loader)
			firstResult <- flightOutcome[int]{value: value, found: found, err: err}
		}()
		<-started

		secondBaseCtx, cancelSecond := context.WithCancel(context.Background())
		secondCtx := &observedDoneContext{Context: secondBaseCtx, observed: make(chan struct{})}
		secondResult := make(chan flightOutcome[int], 1)
		go func() {
			value, found, err := group.Do(secondCtx, "key", loader)
			secondResult <- flightOutcome[int]{value: value, found: found, err: err}
		}()
		<-secondCtx.observed

		close(release)
		cancelFirst()
		cancelSecond()
		<-firstResult
		<-secondResult

		value, found, err := group.Do(context.Background(), "key", loader)
		require.NoError(t, err, "iteration %d", iteration)
		assert.True(t, found, "iteration %d", iteration)
		assert.Equal(t, 2, value, "iteration %d", iteration)
		assert.Equal(t, int32(2), calls.Load(), "iteration %d", iteration)
	}
}

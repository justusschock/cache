package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type flightOutcome[V any] struct {
	value V
	found bool
	err   error
}

// FR-CACHE-09
func TestFlightSharesTypedResultAndLetsWaiterCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var group flightGroup[string, int]
		release := make(chan struct{})
		var calls atomic.Int32
		loader := func(context.Context) (int, bool, error) {
			calls.Add(1)
			<-release
			return 42, true, nil
		}

		owner := make(chan int, 1)
		go func() {
			value, _, _ := group.Do(context.Background(), "key", loader)
			owner <- value
		}()
		synctest.Wait()

		waiterCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := group.Do(waiterCtx, "key", loader)
		require.ErrorIs(t, err, context.Canceled)

		close(release)
		assert.Equal(t, 42, <-owner)
		assert.Equal(t, int32(1), calls.Load())
	})
}

// FR-CACHE-10
func TestFlightPreservesLoaderError(t *testing.T) {
	var group flightGroup[string, int]
	wantErr := errors.New("load failed")

	_, found, err := group.Do(context.Background(), "key", func(context.Context) (int, bool, error) {
		return 0, false, wantErr
	})

	assert.False(t, found)
	assert.ErrorIs(t, err, wantErr)
}

// FR-CACHE-09
func TestFlightCancelsFillOnlyAfterLastWaiterDeparts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var group flightGroup[string, int]
		canceled := make(chan struct{})
		var calls atomic.Int32
		loader := func(ctx context.Context) (int, bool, error) {
			if calls.Add(1) == 1 {
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
		synctest.Wait()

		secondCtx, cancelSecond := context.WithCancel(context.Background())
		secondResult := make(chan flightOutcome[int], 1)
		go func() {
			value, found, err := group.Do(secondCtx, "key", loader)
			secondResult <- flightOutcome[int]{value: value, found: found, err: err}
		}()
		synctest.Wait()

		cancelFirst()
		assert.ErrorIs(t, (<-firstResult).err, context.Canceled)
		synctest.Wait()
		select {
		case <-canceled:
			assert.Fail(t, "fill canceled while another waiter was active")
		default:
		}

		cancelSecond()
		assert.ErrorIs(t, (<-secondResult).err, context.Canceled)
		synctest.Wait()
		select {
		case <-canceled:
		default:
			require.Fail(t, "fill context remained active after its last waiter departed")
		}

		value, found, err := group.Do(context.Background(), "key", loader)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 42, value)
		assert.Equal(t, int32(2), calls.Load())
	})
}

// FR-CACHE-09
func TestFlightCompletionAndDepartureAlwaysStartLaterLifecycleFresh(t *testing.T) {
	for iteration := range 100 {
		synctest.Test(t, func(t *testing.T) {
			var group flightGroup[string, int]
			release := make(chan struct{})
			var calls atomic.Int32
			loader := func(context.Context) (int, bool, error) {
				call := calls.Add(1)
				if call == 1 {
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
			synctest.Wait()

			secondCtx, cancelSecond := context.WithCancel(context.Background())
			secondResult := make(chan flightOutcome[int], 1)
			go func() {
				value, found, err := group.Do(secondCtx, "key", loader)
				secondResult <- flightOutcome[int]{value: value, found: found, err: err}
			}()
			synctest.Wait()

			// Completion and departure race; whichever wins, the next Do must
			// start a fresh lifecycle.
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
		})
	}
}

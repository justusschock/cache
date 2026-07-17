package cache

import (
	"context"
	"sync"

	"github.com/justusschock/go-concurrency"
)

type loadResult[V any] struct {
	value V
	found bool
}

type flightCall[V any] struct {
	future  *concurrency.Future[loadResult[V]]
	cancel  context.CancelFunc
	waiters int
}

type flightGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*flightCall[V]
}

func (g *flightGroup[K, V]) Do(
	ctx context.Context,
	key K,
	fn func(context.Context) (V, bool, error),
) (V, bool, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[K]*flightCall[V])
	}
	call, ok := g.calls[key]
	if ok {
		_, _, complete := call.future.Try()
		if complete {
			delete(g.calls, key)
			call.cancel()
			ok = false
		}
	}
	if !ok {
		loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		call = &flightCall[V]{cancel: cancel}
		call.future = concurrency.Go(loadCtx, func(ctx context.Context) (loadResult[V], error) {
			value, found, err := fn(ctx)
			return loadResult[V]{value: value, found: found}, err
		})
		g.calls[key] = call
	}
	call.waiters++
	g.mu.Unlock()

	result, err := call.future.Await(ctx)
	g.depart(key, call)
	return result.value, result.found, err
}

func (g *flightGroup[K, V]) depart(key K, call *flightCall[V]) {
	g.mu.Lock()
	call.waiters--
	if call.waiters == 0 {
		if g.calls[key] == call {
			delete(g.calls, key)
		}
		call.cancel()
	}
	g.mu.Unlock()
}

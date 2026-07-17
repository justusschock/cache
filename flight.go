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

type flightGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*concurrency.Future[loadResult[V]]
}

func (g *flightGroup[K, V]) Do(
	ctx context.Context,
	key K,
	fn func(context.Context) (V, bool, error),
) (V, bool, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[K]*concurrency.Future[loadResult[V]])
	}
	future, ok := g.calls[key]
	if ok {
		_, _, complete := future.Try()
		if complete {
			delete(g.calls, key)
			ok = false
		}
	}
	if !ok {
		loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		future = concurrency.Go(loadCtx, func(ctx context.Context) (loadResult[V], error) {
			value, found, err := fn(ctx)
			return loadResult[V]{value: value, found: found}, err
		})
		g.calls[key] = future
		go g.removeWhenDone(key, future, cancel)
	}
	g.mu.Unlock()

	result, err := future.Await(ctx)
	return result.value, result.found, err
}

func (g *flightGroup[K, V]) removeWhenDone(
	key K,
	future *concurrency.Future[loadResult[V]],
	cancel context.CancelFunc,
) {
	<-future.Done()
	g.mu.Lock()
	if g.calls[key] == future {
		delete(g.calls, key)
	}
	g.mu.Unlock()
	cancel()
}

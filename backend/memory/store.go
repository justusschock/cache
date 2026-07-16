// Package memory provides a race-safe in-process cache store.
package memory

import (
	"context"
	"sync"
	"time"

	cache "github.com/justusschock/go-cache"
)

type record[V any] struct {
	entry      cache.Entry[V]
	generation uint64
}

type namespaceState[K comparable, V any] struct {
	generation uint64
	entries    map[K]record[V]
}

// Store is an in-memory cache store protected by a mutex.
type Store[K comparable, V any] struct {
	mu         sync.Mutex
	namespaces map[string]*namespaceState[K, V]
}

// New constructs an empty in-memory store.
func New[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{namespaces: make(map[string]*namespaceState[K, V])}
}

func (s *Store[K, V]) Get(ctx context.Context, namespace string, key K) (cache.Entry[V], bool, error) {
	var zero cache.Entry[V]
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}

	state, ok := s.namespaces[namespace]
	if !ok {
		return zero, false, nil
	}
	record, ok := state.entries[key]
	if !ok || record.generation != state.generation {
		return zero, false, nil
	}
	if !record.entry.ExpiresAt.After(time.Now()) {
		delete(state.entries, key)
		return zero, false, nil
	}
	return record.entry, true, nil
}

func (s *Store[K, V]) Set(ctx context.Context, namespace string, key K, entry cache.Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	state := s.namespace(namespace)
	state.entries[key] = record[V]{entry: entry, generation: state.generation}
	return nil
}

func (s *Store[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if state, ok := s.namespaces[namespace]; ok {
		delete(state.entries, key)
	}
	return nil
}

func (s *Store[K, V]) InvalidateAll(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	state := s.namespace(namespace)
	state.generation++
	state.entries = make(map[K]record[V])
	return nil
}

func (s *Store[K, V]) namespace(namespace string) *namespaceState[K, V] {
	state, ok := s.namespaces[namespace]
	if !ok {
		state = &namespaceState[K, V]{entries: make(map[K]record[V])}
		s.namespaces[namespace] = state
	}
	return state
}

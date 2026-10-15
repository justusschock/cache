// Package memory provides a race-safe in-process cache store.
package memory

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	cache "github.com/justusschock/go-cache"
)

var ErrInvalidOptions = errors.New("cache memory: invalid options")

const (
	defaultMaxEntries      = 10_000
	defaultCleanupInterval = time.Minute
)

type Options struct {
	MaxEntries      int
	CleanupInterval time.Duration
}

type itemKey[K comparable] struct {
	namespace string
	key       K
}

type record[K comparable, V any] struct {
	entry      cache.Entry[V]
	generation uint64
	element    *list.Element
}

type namespaceState[K comparable, V any] struct {
	generation uint64
	entries    map[K]record[K, V]
}

// Store is an in-memory cache store protected by a mutex.
type Store[K comparable, V any] struct {
	mu         sync.Mutex
	namespaces map[string]*namespaceState[K, V]
	lru        *list.List
	maxEntries int
	stop       chan struct{}
	done       chan struct{}
	closeOnce  sync.Once
}

// New constructs an empty in-memory store with bounded defaults.
func New[K comparable, V any]() *Store[K, V] {
	store, _ := NewWithOptions[K, V](Options{
		MaxEntries:      defaultMaxEntries,
		CleanupInterval: defaultCleanupInterval,
	})
	return store
}

func NewWithOptions[K comparable, V any](options Options) (*Store[K, V], error) {
	if options.MaxEntries <= 0 || options.CleanupInterval <= 0 {
		return nil, ErrInvalidOptions
	}
	store := &Store[K, V]{
		namespaces: make(map[string]*namespaceState[K, V]),
		lru:        list.New(),
		maxEntries: options.MaxEntries,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go store.sweep(options.CleanupInterval)
	return store, nil
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
	entry, present := s.getLocked(namespace, key, time.Now())
	return entry, present, nil
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
	s.setLocked(namespace, key, entry)
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
	s.invalidateLocked(namespace, key)
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
	state, ok := s.namespaces[namespace]
	if !ok {
		return nil
	}
	for key, record := range state.entries {
		s.remove(namespace, state, key, record)
	}
	return nil
}

// GetMany looks up keys under one lock acquisition. Results are aligned with keys.
func (s *Store[K, V]) GetMany(ctx context.Context, namespace string, keys []K) ([]cache.Entry[V], []bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	entries := make([]cache.Entry[V], len(keys))
	present := make([]bool, len(keys))
	now := time.Now()
	for index, key := range keys {
		entries[index], present[index] = s.getLocked(namespace, key, now)
	}
	return entries, present, nil
}

// SetMany stores entries[i] under keys[i] under one lock acquisition, so no
// other store operation observes a partially applied batch. Capacity eviction
// still applies to the resulting entries.
func (s *Store[K, V]) SetMany(ctx context.Context, namespace string, keys []K, entries []cache.Entry[V]) error {
	if len(keys) != len(entries) {
		return cache.ErrBatchLength
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for index, key := range keys {
		s.setLocked(namespace, key, entries[index])
	}
	return nil
}

// InvalidateMany removes keys under one lock acquisition.
func (s *Store[K, V]) InvalidateMany(ctx context.Context, namespace string, keys []K) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, key := range keys {
		s.invalidateLocked(namespace, key)
	}
	return nil
}

func (s *Store[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}

func (s *Store[K, V]) Close() error {
	s.closeOnce.Do(func() { close(s.stop) })
	<-s.done
	return nil
}

func (s *Store[K, V]) getLocked(namespace string, key K, now time.Time) (cache.Entry[V], bool) {
	var zero cache.Entry[V]
	state, ok := s.namespaces[namespace]
	if !ok {
		return zero, false
	}
	record, ok := state.entries[key]
	if !ok {
		return zero, false
	}
	if record.generation != state.generation || record.entry.Expired(now) {
		s.remove(namespace, state, key, record)
		return zero, false
	}
	s.lru.MoveToFront(record.element)
	return record.entry, true
}

func (s *Store[K, V]) setLocked(namespace string, key K, entry cache.Entry[V]) {
	state := s.namespace(namespace)
	if existing, ok := state.entries[key]; ok {
		existing.entry = entry
		existing.generation = state.generation
		state.entries[key] = existing
		s.lru.MoveToFront(existing.element)
	} else {
		element := s.lru.PushFront(itemKey[K]{namespace: namespace, key: key})
		state.entries[key] = record[K, V]{
			entry:      entry,
			generation: state.generation,
			element:    element,
		}
	}
	for s.lru.Len() > s.maxEntries {
		s.evictOldest()
	}
}

func (s *Store[K, V]) invalidateLocked(namespace string, key K) {
	if state, ok := s.namespaces[namespace]; ok {
		if record, present := state.entries[key]; present {
			s.remove(namespace, state, key, record)
		}
	}
}

func (s *Store[K, V]) namespace(namespace string) *namespaceState[K, V] {
	state, ok := s.namespaces[namespace]
	if !ok {
		state = &namespaceState[K, V]{entries: make(map[K]record[K, V])}
		s.namespaces[namespace] = state
	}
	return state
}

func (s *Store[K, V]) remove(namespace string, state *namespaceState[K, V], key K, record record[K, V]) {
	delete(state.entries, key)
	s.lru.Remove(record.element)
	if len(state.entries) == 0 {
		delete(s.namespaces, namespace)
	}
}

func (s *Store[K, V]) evictOldest() {
	element := s.lru.Back()
	if element == nil {
		return
	}
	item := element.Value.(itemKey[K])
	state, ok := s.namespaces[item.namespace]
	if !ok {
		s.lru.Remove(element)
		return
	}
	record, ok := state.entries[item.key]
	if !ok || record.element != element {
		s.lru.Remove(element)
		return
	}
	s.remove(item.namespace, state, item.key, record)
}

func (s *Store[K, V]) sweep(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(s.done)
	for {
		select {
		case <-ticker.C:
			s.removeExpired(time.Now())
		case <-s.stop:
			return
		}
	}
}

func (s *Store[K, V]) removeExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for namespace, state := range s.namespaces {
		for key, record := range state.entries {
			if record.generation != state.generation || record.entry.Expired(now) {
				s.remove(namespace, state, key, record)
			}
		}
	}
}

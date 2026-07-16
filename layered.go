package cache

import (
	"context"
	"errors"
	"fmt"
)

var ErrNoLayers = errors.New("cache: at least one layer is required")

// Event describes a non-fatal layered-cache failure without exposing keys or values.
type Event struct {
	Operation string
	Namespace string
	Layer     int
	Err       error
}

// Observer receives fail-open and promotion errors.
type Observer interface {
	Observe(context.Context, Event)
}

type layeredConfig struct {
	strictReads bool
	observer    Observer
}

// LayeredOption configures layered-store behavior.
type LayeredOption func(*layeredConfig)

// WithStrictLayerReads makes the first read error abort the lookup.
func WithStrictLayerReads() LayeredOption {
	return func(config *layeredConfig) {
		config.strictReads = true
	}
}

// WithLayerObserver reports errors skipped by fail-open reads and promotions.
func WithLayerObserver(observer Observer) LayeredOption {
	return func(config *layeredConfig) {
		config.observer = observer
	}
}

// LayeredStore composes stores ordered from fastest to slowest.
type LayeredStore[K comparable, V any] struct {
	layers      []Store[K, V]
	strictReads bool
	observer    Observer
}

// NewLayered constructs a store from a fastest-to-slowest layer list.
func NewLayered[K comparable, V any](layers []Store[K, V], options ...LayeredOption) (*LayeredStore[K, V], error) {
	if len(layers) == 0 {
		return nil, ErrNoLayers
	}
	for _, layer := range layers {
		if layer == nil {
			return nil, ErrNilStore
		}
	}
	config := layeredConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	return &LayeredStore[K, V]{
		layers:      append([]Store[K, V](nil), layers...),
		strictReads: config.strictReads,
		observer:    config.observer,
	}, nil
}

func (s *LayeredStore[K, V]) Get(ctx context.Context, namespace string, key K) (Entry[V], bool, error) {
	var zero Entry[V]
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}

	missed := make([]int, 0, len(s.layers))
	for index, layer := range s.layers {
		entry, present, err := layer.Get(ctx, namespace, key)
		if err != nil {
			s.observe(ctx, Event{Operation: "get", Namespace: namespace, Layer: index, Err: err})
			if s.strictReads {
				return zero, false, fmt.Errorf("cache layer %d get: %w", index, err)
			}
			missed = append(missed, index)
			continue
		}
		if !present {
			missed = append(missed, index)
			continue
		}
		for _, missedIndex := range missed {
			if err := s.layers[missedIndex].Set(ctx, namespace, key, entry); err != nil {
				s.observe(ctx, Event{Operation: "promote", Namespace: namespace, Layer: missedIndex, Err: err})
			}
		}
		return entry, true, nil
	}
	return zero, false, nil
}

func (s *LayeredStore[K, V]) Set(ctx context.Context, namespace string, key K, entry Entry[V]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	last := len(s.layers) - 1
	if err := s.layers[last].Set(ctx, namespace, key, entry); err != nil {
		return fmt.Errorf("cache layer %d set: %w", last, err)
	}

	var joined error
	for index := last - 1; index >= 0; index-- {
		if err := s.layers[index].Set(ctx, namespace, key, entry); err != nil {
			joined = errors.Join(joined, fmt.Errorf("cache layer %d set: %w", index, err))
		}
	}
	return joined
}

func (s *LayeredStore[K, V]) Invalidate(ctx context.Context, namespace string, key K) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var joined error
	for index, layer := range s.layers {
		if err := layer.Invalidate(ctx, namespace, key); err != nil {
			joined = errors.Join(joined, fmt.Errorf("cache layer %d invalidate: %w", index, err))
		}
	}
	return joined
}

func (s *LayeredStore[K, V]) InvalidateAll(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var joined error
	for index, layer := range s.layers {
		if err := layer.InvalidateAll(ctx, namespace); err != nil {
			joined = errors.Join(joined, fmt.Errorf("cache layer %d invalidate all: %w", index, err))
		}
	}
	return joined
}

func (s *LayeredStore[K, V]) observe(ctx context.Context, event Event) {
	if s.observer != nil {
		s.observer.Observe(ctx, event)
	}
}

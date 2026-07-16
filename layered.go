package cache

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNoLayers          = errors.New("cache: at least one layer is required")
	ErrNoInvalidationBus = errors.New("cache: no invalidation bus configured")
	ErrInvalidLowerLayer = errors.New("cache: lower layer must have at least one upper layer")
)

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
	bus         InvalidationBus[K]
}

// NewLayered constructs a store from a fastest-to-slowest layer list.
func NewLayered[K comparable, V any](layers []Store[K, V], options ...LayeredOption) (*LayeredStore[K, V], error) {
	return newLayered(layers, nil, options...)
}

// NewLayeredWithInvalidationBus constructs a layered store that publishes invalidations.
func NewLayeredWithInvalidationBus[K comparable, V any](layers []Store[K, V], bus InvalidationBus[K], options ...LayeredOption) (*LayeredStore[K, V], error) {
	if bus == nil {
		return nil, ErrNoInvalidationBus
	}
	return newLayered(layers, bus, options...)
}

func newLayered[K comparable, V any](layers []Store[K, V], bus InvalidationBus[K], options ...LayeredOption) (*LayeredStore[K, V], error) {
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
		bus:         bus,
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
	if s.bus != nil {
		if err := s.bus.Publish(ctx, InvalidationEvent[K]{Namespace: namespace, Kind: InvalidateKey, Key: key}); err != nil {
			joined = errors.Join(joined, fmt.Errorf("cache publish invalidation: %w", err))
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
	if s.bus != nil {
		if err := s.bus.Publish(ctx, InvalidationEvent[K]{Namespace: namespace, Kind: InvalidateNamespace}); err != nil {
			joined = errors.Join(joined, fmt.Errorf("cache publish invalidation: %w", err))
		}
	}
	return joined
}

// ListenInvalidations applies bus events to layers above lowerLayer.
// The lower layer itself is not touched because it is the event's source of truth.
func (s *LayeredStore[K, V]) ListenInvalidations(ctx context.Context, namespace string, lowerLayer int) error {
	if s.bus == nil {
		return ErrNoInvalidationBus
	}
	if lowerLayer <= 0 || lowerLayer >= len(s.layers) {
		return ErrInvalidLowerLayer
	}
	return s.bus.Subscribe(ctx, namespace, func(eventCtx context.Context, event InvalidationEvent[K]) error {
		var joined error
		for index := 0; index < lowerLayer; index++ {
			var err error
			switch event.Kind {
			case InvalidateKey:
				err = s.layers[index].Invalidate(eventCtx, event.Namespace, event.Key)
			case InvalidateNamespace:
				err = s.layers[index].InvalidateAll(eventCtx, event.Namespace)
			default:
				continue
			}
			if err != nil {
				joined = errors.Join(joined, fmt.Errorf("cache layer %d remote invalidate: %w", index, err))
			}
		}
		return joined
	})
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

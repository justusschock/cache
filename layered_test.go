package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type layerLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *layerLog) add(call string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (l *layerLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

type layerStore struct {
	name          string
	log           *layerLog
	entries       map[string]cache.Entry[string]
	getErr        error
	setErr        error
	invalidateErr error
	allErr        error
}

func newLayerStore(name string, log *layerLog) *layerStore {
	return &layerStore{name: name, log: log, entries: make(map[string]cache.Entry[string])}
}

func (s *layerStore) Get(_ context.Context, _ string, key string) (cache.Entry[string], bool, error) {
	s.log.add("get:" + s.name)
	if s.getErr != nil {
		return cache.Entry[string]{}, false, s.getErr
	}
	entry, ok := s.entries[key]
	return entry, ok, nil
}

func (s *layerStore) Set(_ context.Context, _ string, key string, entry cache.Entry[string]) error {
	s.log.add("set:" + s.name)
	if s.setErr != nil {
		return s.setErr
	}
	s.entries[key] = entry
	return nil
}

func (s *layerStore) Invalidate(_ context.Context, _ string, key string) error {
	s.log.add("invalidate:" + s.name)
	delete(s.entries, key)
	return s.invalidateErr
}

func (s *layerStore) InvalidateAll(_ context.Context, _ string) error {
	s.log.add("all:" + s.name)
	clear(s.entries)
	return s.allErr
}

type eventRecorder struct {
	events []cache.Event
}

func (r *eventRecorder) Observe(_ context.Context, event cache.Event) {
	r.events = append(r.events, event)
}

type recordingInvalidationBus[K comparable] struct {
	events     []cache.InvalidationEvent[K]
	deliveries []cache.InvalidationEvent[K]
}

func (b *recordingInvalidationBus[K]) Publish(_ context.Context, event cache.InvalidationEvent[K]) error {
	b.events = append(b.events, event)
	return nil
}

func (b *recordingInvalidationBus[K]) Subscribe(
	ctx context.Context,
	_ string,
	handler cache.InvalidationHandler[K],
) error {
	for _, event := range b.deliveries {
		if err := handler(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func TestLayeredGetPromotesLowerHitWithoutChangingExpiration(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	middle := newLayerStore("middle", log)
	slow := newLayerStore("slow", log)
	expiresAt := time.Now().Add(time.Minute).Round(0)
	slow.entries["alice"] = cache.Entry[string]{Value: "value", Found: true, ExpiresAt: expiresAt}

	layered, err := cache.NewLayered([]cache.Store[string, string]{fast, middle, slow})
	require.NoError(t, err)
	entry, present, err := layered.Get(context.Background(), "users", "alice")
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, "value", entry.Value)
	assert.Equal(t, expiresAt, fast.entries["alice"].ExpiresAt)
	assert.Equal(t, expiresAt, middle.entries["alice"].ExpiresAt)
	assert.Equal(t, []string{"get:fast", "get:middle", "get:slow", "set:fast", "set:middle"}, log.snapshot())
}

func TestLayeredSetWritesSlowestToFastest(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	middle := newLayerStore("middle", log)
	slow := newLayerStore("slow", log)
	layered, err := cache.NewLayered([]cache.Store[string, string]{fast, middle, slow})
	require.NoError(t, err)

	entry := cache.Entry[string]{Value: "value", Found: true, ExpiresAt: time.Now().Add(time.Minute)}
	require.NoError(t, layered.Set(context.Background(), "users", "alice", entry))
	assert.Equal(t, []string{"set:slow", "set:middle", "set:fast"}, log.snapshot())
}

func TestLayeredSetStopsWhenSlowestLayerFails(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	slow := newLayerStore("slow", log)
	slow.setErr = errors.New("slow failed")
	layered, err := cache.NewLayered([]cache.Store[string, string]{fast, slow})
	require.NoError(t, err)

	err = layered.Set(context.Background(), "users", "alice", cache.Entry[string]{})
	assert.ErrorContains(t, err, "slow failed")
	assert.Equal(t, []string{"set:slow"}, log.snapshot())
}

func TestLayeredSetJoinsUpperLayerErrors(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	middle := newLayerStore("middle", log)
	slow := newLayerStore("slow", log)
	fastErr := errors.New("fast failed")
	middleErr := errors.New("middle failed")
	fast.setErr = fastErr
	middle.setErr = middleErr
	layered, err := cache.NewLayered([]cache.Store[string, string]{fast, middle, slow})
	require.NoError(t, err)

	err = layered.Set(context.Background(), "users", "alice", cache.Entry[string]{})
	assert.ErrorIs(t, err, fastErr)
	assert.ErrorIs(t, err, middleErr)
	assert.Equal(t, []string{"set:slow", "set:middle", "set:fast"}, log.snapshot())
}

func TestLayeredPublishesExactInvalidationScopes(t *testing.T) {
	bus := &recordingInvalidationBus[string]{}
	layered, err := cache.NewLayeredWithInvalidationBus(
		[]cache.Store[string, string]{newLayerStore("only", &layerLog{})},
		bus,
	)
	require.NoError(t, err)

	entry := cache.Entry[string]{Value: "value", Found: true, ExpiresAt: time.Now().Add(time.Minute)}
	require.NoError(t, layered.Set(context.Background(), "users", "alice", entry))
	require.NoError(t, layered.Invalidate(context.Background(), "users", "alice"))
	require.NoError(t, layered.InvalidateAll(context.Background(), "users"))

	assert.Equal(t, []cache.InvalidationEvent[string]{
		{Namespace: "users", Kind: cache.InvalidateKey, Key: "alice"},
		{Namespace: "users", Kind: cache.InvalidateKey, Key: "alice"},
		{Namespace: "users", Kind: cache.InvalidateNamespace},
	}, bus.events)
}

func TestListenerTreatsDuplicateAndOutOfOrderEventsAsEvictions(t *testing.T) {
	log := &layerLog{}
	upper := newLayerStore("upper", log)
	lower := newLayerStore("lower", log)
	expiresAt := time.Now().Add(time.Minute)
	upper.entries["alice"] = cache.Entry[string]{Value: "stale", Found: true, ExpiresAt: expiresAt}
	upper.entries["bob"] = cache.Entry[string]{Value: "fresh", Found: true, ExpiresAt: expiresAt}
	bus := &recordingInvalidationBus[string]{deliveries: []cache.InvalidationEvent[string]{
		{Namespace: "users", Kind: cache.InvalidateKey, Key: "alice", Generation: 9},
		{Namespace: "users", Kind: cache.InvalidateKey, Key: "alice", Generation: 2},
		{Namespace: "users", Kind: cache.InvalidateKey, Key: "alice", Generation: 2},
	}}
	layered, err := cache.NewLayeredWithInvalidationBus(
		[]cache.Store[string, string]{upper, lower},
		bus,
	)
	require.NoError(t, err)

	require.NoError(t, layered.ListenInvalidations(context.Background(), "users", 1))
	_, alicePresent := upper.entries["alice"]
	_, bobPresent := upper.entries["bob"]
	assert.False(t, alicePresent)
	assert.True(t, bobPresent)
}

func TestLayeredReadCanFailOpenOrStrict(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	slow := newLayerStore("slow", log)
	fastErr := errors.New("fast unavailable")
	fast.getErr = fastErr
	slow.entries["alice"] = cache.Entry[string]{Value: "value", Found: true, ExpiresAt: time.Now().Add(time.Minute)}
	recorder := &eventRecorder{}

	layered, err := cache.NewLayered(
		[]cache.Store[string, string]{fast, slow},
		cache.WithLayerObserver(recorder),
	)
	require.NoError(t, err)
	entry, present, err := layered.Get(context.Background(), "users", "alice")
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, "value", entry.Value)
	require.Len(t, recorder.events, 1)
	assert.ErrorIs(t, recorder.events[0].Err, fastErr)

	strict, err := cache.NewLayered(
		[]cache.Store[string, string]{fast, slow},
		cache.WithStrictLayerReads(),
	)
	require.NoError(t, err)
	_, _, err = strict.Get(context.Background(), "users", "alice")
	assert.ErrorIs(t, err, fastErr)
}

func TestLayeredTreatsTypedNilOptionalObserverAsAbsent(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	fast.getErr = errors.New("fast unavailable")
	slow := newLayerStore("slow", log)
	slow.entries["alice"] = cache.Entry[string]{Value: "value", Found: true, ExpiresAt: time.Now().Add(time.Minute)}
	var observer *eventRecorder
	layered, err := cache.NewLayered(
		[]cache.Store[string, string]{fast, slow},
		cache.WithLayerObserver(observer),
	)
	require.NoError(t, err)

	var entry cache.Entry[string]
	var present bool
	assert.NotPanics(t, func() {
		entry, present, err = layered.Get(context.Background(), "users", "alice")
	})
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, "value", entry.Value)
}

func TestLayeredInvalidationAttemptsEveryLayerAndJoinsErrors(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	middle := newLayerStore("middle", log)
	slow := newLayerStore("slow", log)
	fastErr := errors.New("fast failed")
	slowErr := errors.New("slow failed")
	fast.invalidateErr = fastErr
	slow.invalidateErr = slowErr
	layered, err := cache.NewLayered([]cache.Store[string, string]{fast, middle, slow})
	require.NoError(t, err)

	err = layered.Invalidate(context.Background(), "users", "alice")
	assert.ErrorIs(t, err, fastErr)
	assert.ErrorIs(t, err, slowErr)
	assert.Equal(t, []string{"invalidate:fast", "invalidate:middle", "invalidate:slow"}, log.snapshot())
}

func TestLayeredInvalidateAllAttemptsEveryLayer(t *testing.T) {
	log := &layerLog{}
	fast := newLayerStore("fast", log)
	slow := newLayerStore("slow", log)
	fastErr := errors.New("fast failed")
	fast.allErr = fastErr
	layered, err := cache.NewLayered([]cache.Store[string, string]{fast, slow})
	require.NoError(t, err)

	err = layered.InvalidateAll(context.Background(), "users")
	assert.ErrorIs(t, err, fastErr)
	assert.Equal(t, []string{"all:fast", "all:slow"}, log.snapshot())
}

func TestNewLayeredRejectsMissingOrNilLayers(t *testing.T) {
	_, err := cache.NewLayered[string, string](nil)
	assert.ErrorIs(t, err, cache.ErrNoLayers)

	_, err = cache.NewLayered([]cache.Store[string, string]{nil})
	assert.ErrorIs(t, err, cache.ErrNilStore)
}

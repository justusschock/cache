package cache_test

import (
	"context"
	"testing"
	"time"

	cache "github.com/justusschock/cache"
	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/messaging"
	"github.com/stretchr/testify/assert"
)

type nilStore struct{}

func (*nilStore) Get(context.Context, string, string) (cache.Entry[int], bool, error) {
	return cache.Entry[int]{}, false, nil
}
func (*nilStore) Set(context.Context, string, string, cache.Entry[int]) error { return nil }
func (*nilStore) Invalidate(context.Context, string, string) error            { return nil }
func (*nilStore) InvalidateAll(context.Context, string) error                 { return nil }

type nilCache struct{}

func (*nilCache) Get(context.Context, string) (int, bool, error)               { return 0, false, nil }
func (*nilCache) Set(context.Context, string, int) error                       { return nil }
func (*nilCache) SetWithTTL(context.Context, string, int, time.Duration) error { return nil }
func (*nilCache) Invalidate(context.Context, string) error                     { return nil }
func (*nilCache) InvalidateAll(context.Context) error                          { return nil }

type nilLocker struct{}

func (*nilLocker) Lock(context.Context, string, string, time.Duration) (cache.Lease, error) {
	return nil, nil
}

type nilCoordinationBackend struct{}

func (*nilCoordinationBackend) Acquire(context.Context, coordination.AcquireRequest) (coordination.Lease, error) {
	return nil, nil
}

type nilCoordinationLease struct{}

func (*nilCoordinationLease) Info() coordination.LeaseInfo { return coordination.LeaseInfo{} }
func (*nilCoordinationLease) Done() <-chan struct{}        { return nil }
func (*nilCoordinationLease) Renew(context.Context, time.Duration) (coordination.LeaseInfo, error) {
	return coordination.LeaseInfo{}, nil
}
func (*nilCoordinationLease) Release(context.Context) error { return nil }

type nilLeaseBackend struct{}

func (nilLeaseBackend) Acquire(context.Context, coordination.AcquireRequest) (coordination.Lease, error) {
	var lease *nilCoordinationLease
	return lease, nil
}

type nilPublisher struct{}

func (*nilPublisher) Publish(context.Context, messaging.Envelope[cache.InvalidationEvent[string]]) error {
	return nil
}

type nilSubscriber struct{}

func (*nilSubscriber) Subscribe(context.Context, messaging.Subscription, messaging.Handler[cache.InvalidationEvent[string]]) error {
	return nil
}

// FR-CACHE-22
func TestConstructorsRejectTypedNilDependencies(t *testing.T) {
	var store *nilStore
	_, err := cache.New("users", time.Minute, cache.Store[string, int](store))
	assert.ErrorIs(t, err, cache.ErrNilStore)
	_, err = cache.NewLayered([]cache.Store[string, int]{store})
	assert.ErrorIs(t, err, cache.ErrNilStore)

	var wrapped *nilCache
	loader := func(context.Context, string) (int, bool, error) { return 0, false, nil }
	_, err = cache.NewPullThrough("users", cache.Cache[string, int](wrapped), loader)
	assert.ErrorIs(t, err, cache.ErrNilCache)

	var locker *nilLocker
	_, err = cache.NewPullThrough("users", &nilCache{}, loader,
		cache.WithLocker[string](cache.Locker[string](locker), time.Minute))
	assert.ErrorIs(t, err, cache.ErrNilLocker)

	var backend *nilCoordinationBackend
	key := func(_, key string) (string, error) { return key, nil }
	_, err = cache.NewCoordinationLocker[string](coordination.Backend(backend), "worker", key)
	assert.ErrorIs(t, err, cache.ErrNilCoordinationBackend)

	coordinationLocker, err := cache.NewCoordinationLocker[string](nilLeaseBackend{}, "worker", key)
	assert.NoError(t, err)
	_, err = coordinationLocker.Lock(context.Background(), "users", "alice", time.Minute)
	assert.ErrorIs(t, err, cache.ErrNilCoordinationLease)

	factory := func(string) messaging.Subscription {
		return messaging.Subscription{Subject: "cache", Consumer: "worker", Semantics: messaging.AtLeastOnce, MaxInFlight: 1}
	}
	var publisher *nilPublisher
	_, err = cache.NewMessagingInvalidationBus[string](publisher, &nilSubscriber{}, factory)
	assert.ErrorIs(t, err, cache.ErrNilMessagingPublisher)
	var subscriber *nilSubscriber
	_, err = cache.NewMessagingInvalidationBus[string](&nilPublisher{}, subscriber, factory)
	assert.ErrorIs(t, err, cache.ErrNilMessagingSubscriber)
}

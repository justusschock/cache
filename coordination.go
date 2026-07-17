package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

var (
	ErrNilCoordinationBackend = errors.New("cache: coordination backend must not be nil")
	ErrEmptyCoordinationOwner = errors.New("cache: coordination owner must not be empty")
	ErrNilFillKeyFunc         = errors.New("cache: fill key function must not be nil")
	ErrNilCoordinationLease   = errors.New("cache: coordination backend returned a nil lease")
)

type FillKeyFunc[K comparable] func(namespace string, key K) (string, error)

type CoordinationLocker[K comparable] struct {
	backend coordination.Backend
	owner   string
	key     FillKeyFunc[K]
}

func NewCoordinationLocker[K comparable](
	backend coordination.Backend,
	owner string,
	key FillKeyFunc[K],
) (*CoordinationLocker[K], error) {
	if backend == nil {
		return nil, ErrNilCoordinationBackend
	}
	if owner == "" {
		return nil, ErrEmptyCoordinationOwner
	}
	if key == nil {
		return nil, ErrNilFillKeyFunc
	}
	return &CoordinationLocker[K]{backend: backend, owner: owner, key: key}, nil
}

func (l *CoordinationLocker[K]) Lock(
	ctx context.Context,
	namespace string,
	key K,
	ttl time.Duration,
) (Lease, error) {
	if ttl <= 0 {
		return nil, ErrInvalidTTL
	}
	fillKey, err := l.key(namespace, key)
	if err != nil {
		return nil, fmt.Errorf("cache coordination fill key: %w", err)
	}
	lease, err := l.backend.Acquire(ctx, coordination.AcquireRequest{
		Key:   fillKey,
		Owner: l.owner,
		TTL:   ttl,
		Wait:  true,
	})
	if err != nil {
		return nil, mapCoordinationError(err)
	}
	if lease == nil {
		return nil, ErrNilCoordinationLease
	}
	return &coordinationLease{lease: lease}, nil
}

type coordinationLease struct {
	lease coordination.Lease
}

func (l *coordinationLease) Done() <-chan struct{} {
	return l.lease.Done()
}

func (l *coordinationLease) Refresh(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	_, err := l.lease.Renew(ctx, ttl)
	return mapCoordinationError(err)
}

func (l *coordinationLease) Release(ctx context.Context) error {
	return mapCoordinationError(l.lease.Release(ctx))
}

func mapCoordinationError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, coordination.ErrNotAcquired):
		return errors.Join(ErrLockNotAcquired, err)
	case errors.Is(err, coordination.ErrLeaseLost), errors.Is(err, coordination.ErrNotOwner):
		return errors.Join(ErrLeaseLost, err)
	default:
		return err
	}
}

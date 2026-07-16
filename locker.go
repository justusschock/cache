package cache

import (
	"context"
	"errors"
	"time"
)

var ErrLockNotAcquired = errors.New("cache: lock not acquired")

// Locker coordinates cache fills for one namespaced key.
type Locker[K comparable] interface {
	Lock(context.Context, string, K, time.Duration) (Lease, error)
}

// Lease is a renewable distributed lock held by the caller.
type Lease interface {
	Refresh(context.Context, time.Duration) error
	Release(context.Context) error
}

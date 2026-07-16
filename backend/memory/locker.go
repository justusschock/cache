package memory

import (
	"context"
	"sync"
	"time"

	cache "github.com/justusschock/go-cache"
)

type lockRecord struct {
	token     uint64
	expiresAt time.Time
}

// Locker provides race-safe in-process miss coordination.
type Locker[K comparable] struct {
	mu        sync.Mutex
	nextToken uint64
	locks     map[string]map[K]lockRecord
}

// NewLocker constructs an empty in-process locker.
func NewLocker[K comparable]() *Locker[K] {
	return &Locker[K]{locks: make(map[string]map[K]lockRecord)}
}

func (l *Locker[K]) Lock(ctx context.Context, namespace string, key K, ttl time.Duration) (cache.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ttl <= 0 {
		return nil, cache.ErrInvalidTTL
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespaceLocks := l.locks[namespace]
	if namespaceLocks == nil {
		namespaceLocks = make(map[K]lockRecord)
		l.locks[namespace] = namespaceLocks
	}
	if current, ok := namespaceLocks[key]; ok && current.expiresAt.After(time.Now()) {
		return nil, cache.ErrLockNotAcquired
	}
	l.nextToken++
	record := lockRecord{token: l.nextToken, expiresAt: time.Now().Add(ttl)}
	namespaceLocks[key] = record
	return &lease[K]{locker: l, namespace: namespace, key: key, token: record.token}, nil
}

type lease[K comparable] struct {
	locker    *Locker[K]
	namespace string
	key       K
	token     uint64
}

func (l *lease[K]) Refresh(ctx context.Context, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return cache.ErrInvalidTTL
	}
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	namespaceLocks := l.locker.locks[l.namespace]
	record, ok := namespaceLocks[l.key]
	if !ok || record.token != l.token || !record.expiresAt.After(time.Now()) {
		return cache.ErrLeaseLost
	}
	record.expiresAt = time.Now().Add(ttl)
	namespaceLocks[l.key] = record
	return nil
}

func (l *lease[K]) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	namespaceLocks := l.locker.locks[l.namespace]
	record, ok := namespaceLocks[l.key]
	if !ok || record.token != l.token {
		return cache.ErrLeaseLost
	}
	delete(namespaceLocks, l.key)
	if len(namespaceLocks) == 0 {
		delete(l.locker.locks, l.namespace)
	}
	return nil
}

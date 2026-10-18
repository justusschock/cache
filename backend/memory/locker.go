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

// Lock acquires a lease on key in namespace for ttl. It returns
// cache.ErrLockNotAcquired while another lease is live and
// cache.ErrInvalidTTL for a non-positive ttl.
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
	now := time.Now()
	l.removeExpired(now)
	namespaceLocks := l.locks[namespace]
	if namespaceLocks == nil {
		namespaceLocks = make(map[K]lockRecord)
		l.locks[namespace] = namespaceLocks
	}
	if current, ok := namespaceLocks[key]; ok && current.expiresAt.After(now) {
		return nil, cache.ErrLockNotAcquired
	}
	l.nextToken++
	record := lockRecord{token: l.nextToken, expiresAt: now.Add(ttl)}
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
	if !ok || record.token != l.token {
		return cache.ErrLeaseLost
	}
	now := time.Now()
	if !record.expiresAt.After(now) {
		l.locker.remove(l.namespace, l.key)
		return cache.ErrLeaseLost
	}
	record.expiresAt = now.Add(ttl)
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
	l.locker.remove(l.namespace, l.key)
	if !record.expiresAt.After(time.Now()) {
		return cache.ErrLeaseLost
	}
	return nil
}

func (l *Locker[K]) remove(namespace string, key K) {
	namespaceLocks := l.locks[namespace]
	delete(namespaceLocks, key)
	if len(namespaceLocks) == 0 {
		delete(l.locks, namespace)
	}
}

func (l *Locker[K]) removeExpired(now time.Time) {
	for namespace, namespaceLocks := range l.locks {
		for key, record := range namespaceLocks {
			if !record.expiresAt.After(now) {
				delete(namespaceLocks, key)
			}
		}
		if len(namespaceLocks) == 0 {
			delete(l.locks, namespace)
		}
	}
}

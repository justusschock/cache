package memcached

import (
	"testing"
	"testing/synctest"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
)

// FR-CACHE-19
func TestExpirationRoundsSubsecondTTLUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assert.Equal(t, int32(1), expiration(time.Now().Add(100*time.Millisecond)))
		assert.Equal(t, int32(2), expiration(time.Now().Add(time.Second+time.Nanosecond)))
		assert.Equal(t, int32(1), expiration(time.Now().Add(-time.Second)))
	})
}

// FR-CACHE-19
func TestExpirationUsesAbsoluteTimeBeyondThirtyDays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		thirtyDays := 30 * 24 * time.Hour
		assert.Equal(t, int32(thirtyDays/time.Second), expiration(time.Now().Add(thirtyDays)))
		beyond := time.Now().Add(thirtyDays + time.Second)
		assert.Equal(t, int32(beyond.Unix()), expiration(beyond))
	})
}

// FR-CACHE-19
func TestExpirationReliesOnEnvelopePastMemcachedUnixRange(t *testing.T) {
	assert.Zero(t, expiration(time.Date(2040, time.January, 1, 0, 0, 0, 0, time.UTC)))
}

// FR-CACHE-03, FR-CACHE-19
func TestEntryExpirationUsesZeroForNoExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assert.Zero(t, entryExpiration(cache.Entry[int]{Value: 1, Found: true}))
		assert.Equal(t, int32(60), entryExpiration(cache.Entry[int]{ExpiresAt: time.Now().Add(time.Minute)}))
	})
}

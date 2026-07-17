package memcached

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExpirationRoundsSubsecondTTLUp(t *testing.T) {
	assert.Equal(t, int32(1), expiration(time.Now().Add(100*time.Millisecond)))
}

func TestExpirationReliesOnEnvelopePastMemcachedUnixRange(t *testing.T) {
	assert.Zero(t, expiration(time.Date(2040, time.January, 1, 0, 0, 0, 0, time.UTC)))
}

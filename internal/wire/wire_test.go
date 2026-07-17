package wire

import (
	"encoding/binary"
	"testing"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntryRoundTripsExpiryOutsideUnixNanoRange(t *testing.T) {
	encoder := New(cache.DefaultCodec[string](), cache.DefaultCodec[int]())
	expiresAt := time.Date(2500, time.January, 2, 3, 4, 5, 678_901_234, time.UTC)
	entry := cache.Entry[int]{Value: 42, Found: true, ExpiresAt: expiresAt}

	envelope, err := encoder.Encode(entry, 7)
	require.NoError(t, err)
	decoded, generation, err := encoder.Decode(envelope)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), generation)
	assert.Equal(t, entry, decoded)
}

func TestDecodeAcceptsVersionOneEntry(t *testing.T) {
	encoder := New(cache.DefaultCodec[string](), cache.DefaultCodec[int]())
	expiresAt := time.Date(2030, time.January, 2, 3, 4, 5, 6, time.UTC)
	payload, err := cache.DefaultCodec[int]().Encode(42)
	require.NoError(t, err)
	envelope := make([]byte, 18+len(payload))
	envelope[0] = 1
	binary.BigEndian.PutUint64(envelope[1:9], 9)
	binary.BigEndian.PutUint64(envelope[9:17], uint64(expiresAt.UnixNano()))
	envelope[17] = flagFound
	copy(envelope[18:], payload)

	decoded, generation, err := encoder.Decode(envelope)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), generation)
	assert.Equal(t, expiresAt, decoded.ExpiresAt)
	assert.Equal(t, 42, decoded.Value)
}

package wire_test

import (
	"testing"

	cache "github.com/justusschock/cache"
	"github.com/justusschock/cache/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FR-CACHE-21
func TestInvalidationRoundTrip(t *testing.T) {
	event := cache.InvalidationEvent[string]{
		Namespace:  "users",
		Kind:       cache.InvalidateKey,
		Key:        "alice",
		Generation: 12,
	}
	encoded, err := wire.EncodeInvalidation(event, cache.DefaultCodec[string]())
	require.NoError(t, err)

	decoded, err := wire.DecodeInvalidation(encoded, cache.DefaultCodec[string]())
	require.NoError(t, err)
	assert.Equal(t, event, decoded)
}

// FR-CACHE-21
func TestNamespaceInvalidationRoundTrip(t *testing.T) {
	event := cache.InvalidationEvent[int]{Namespace: "users", Kind: cache.InvalidateNamespace}
	encoded, err := wire.EncodeInvalidation(event, cache.DefaultCodec[int]())
	require.NoError(t, err)

	decoded, err := wire.DecodeInvalidation(encoded, cache.DefaultCodec[int]())
	require.NoError(t, err)
	assert.Equal(t, event, decoded)
}

// FR-CACHE-21
func TestInvalidationRejectsMalformedPayload(t *testing.T) {
	_, err := wire.DecodeInvalidation([]byte("invalid"), cache.DefaultCodec[string]())
	assert.Error(t, err)
}

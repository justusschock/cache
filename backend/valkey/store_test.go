package valkey_test

import (
	"context"
	"testing"
	"time"

	valkeycache "github.com/justusschock/cache/backend/valkey"
	"github.com/stretchr/testify/assert"
	valkeyclient "github.com/valkey-io/valkey-go"
)

type nilCodec[T any] struct{}

func (*nilCodec[T]) Encode(T) ([]byte, error) { return nil, nil }
func (*nilCodec[T]) Decode([]byte) (T, error) {
	var zero T
	return zero, nil
}

type nilClient struct{}

func (*nilClient) B() valkeyclient.Builder { return valkeyclient.Builder{} }
func (*nilClient) Do(context.Context, valkeyclient.Completed) valkeyclient.ValkeyResult {
	return valkeyclient.ValkeyResult{}
}
func (*nilClient) DoMulti(context.Context, ...valkeyclient.Completed) []valkeyclient.ValkeyResult {
	return nil
}
func (*nilClient) Receive(context.Context, valkeyclient.Completed, func(valkeyclient.PubSubMessage)) error {
	return nil
}
func (*nilClient) Close() {}
func (*nilClient) DoCache(context.Context, valkeyclient.Cacheable, time.Duration) valkeyclient.ValkeyResult {
	return valkeyclient.ValkeyResult{}
}
func (*nilClient) DoMultiCache(context.Context, ...valkeyclient.CacheableTTL) []valkeyclient.ValkeyResult {
	return nil
}
func (*nilClient) DoStream(context.Context, valkeyclient.Completed) valkeyclient.ValkeyResultStream {
	return valkeyclient.ValkeyResultStream{}
}
func (*nilClient) DoMultiStream(context.Context, ...valkeyclient.Completed) valkeyclient.MultiValkeyResultStream {
	return valkeyclient.MultiValkeyResultStream{}
}
func (*nilClient) Dedicated(func(valkeyclient.DedicatedClient) error) error { return nil }
func (*nilClient) Dedicate() (valkeyclient.DedicatedClient, func())         { return nil, func() {} }
func (*nilClient) Nodes() map[string]valkeyclient.Client                    { return nil }

// FR-CACHE-22
func TestNewRejectsNilClientAndCodecs(t *testing.T) {
	_, err := valkeycache.New[string, int](nil)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	_, err = valkeycache.New[string, int](nil, valkeycache.WithKeyCodec[string, int](nil))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
	_, err = valkeycache.New[string, int](nil, valkeycache.WithValueCodec[string, int](nil))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
	var client *nilClient
	_, err = valkeycache.New[string, int](client)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	var keyCodec *nilCodec[string]
	_, err = valkeycache.New[string, int](nil, valkeycache.WithKeyCodec[string, int](keyCodec))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
	var valueCodec *nilCodec[int]
	_, err = valkeycache.New[string, int](nil, valkeycache.WithValueCodec[string, int](valueCodec))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
}

// FR-CACHE-22
func TestNewLockerRejectsNilClient(t *testing.T) {
	_, err := valkeycache.NewLocker[string](nil)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	var client *nilClient
	_, err = valkeycache.NewLocker[string](client)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	var codec *nilCodec[string]
	_, err = valkeycache.NewLocker(nil, valkeycache.WithLockerKeyCodec[string](codec))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
}

// FR-CACHE-22
func TestNewInvalidationBusRejectsNilClientAndCodec(t *testing.T) {
	_, err := valkeycache.NewInvalidationBus[string](nil)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	_, err = valkeycache.NewInvalidationBus(nil, valkeycache.WithInvalidationKeyCodec[string](nil))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
	var client *nilClient
	_, err = valkeycache.NewInvalidationBus[string](client)
	assert.ErrorIs(t, err, valkeycache.ErrNilClient)
	var codec *nilCodec[string]
	_, err = valkeycache.NewInvalidationBus(nil, valkeycache.WithInvalidationKeyCodec[string](codec))
	assert.ErrorIs(t, err, valkeycache.ErrNilCodec)
}

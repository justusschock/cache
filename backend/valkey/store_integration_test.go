//go:build integration

package valkey_test

import (
	"context"
	"os"
	"testing"

	cache "github.com/justusschock/go-cache"
	valkeycache "github.com/justusschock/go-cache/backend/valkey"
	"github.com/justusschock/go-cache/internal/cachetest"
	"github.com/stretchr/testify/require"
	valkeyclient "github.com/valkey-io/valkey-go"
)

func TestStoreIntegrationContract(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		address = "127.0.0.1:6381"
	}
	client, err := valkeyclient.NewClient(valkeyclient.ClientOption{InitAddress: []string{address}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.NoError(t, client.Do(context.Background(), client.B().Ping().Build()).Error())

	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		store, storeErr := valkeycache.New[string, int](client)
		require.NoError(t, storeErr)
		return store
	})
}

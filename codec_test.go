package cache_test

import (
	"errors"
	"testing"

	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type codecValue struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type codecInt int64

func TestDefaultCodecRoundTripsTypedValues(t *testing.T) {
	tests := []struct {
		name  string
		value codecValue
	}{
		{name: "struct", value: codecValue{Name: "alice", Count: 42}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codec := cache.DefaultCodec[codecValue]()
			encoded, err := codec.Encode(test.value)
			require.NoError(t, err)
			decoded, err := codec.Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, test.value, decoded)
		})
	}
}

func TestDefaultCodecUsesDirectRepresentations(t *testing.T) {
	stringCodec := cache.DefaultCodec[string]()
	encodedString, err := stringCodec.Encode("hello")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), encodedString)
	decodedString, err := stringCodec.Decode(encodedString)
	require.NoError(t, err)
	assert.Equal(t, "hello", decodedString)

	bytesCodec := cache.DefaultCodec[[]byte]()
	encodedBytes, err := bytesCodec.Encode([]byte{0, 1, 2})
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 1, 2}, encodedBytes)
	decodedBytes, err := bytesCodec.Decode(encodedBytes)
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 1, 2}, decodedBytes)

	intCodec := cache.DefaultCodec[codecInt]()
	encodedInt, err := intCodec.Encode(codecInt(-42))
	require.NoError(t, err)
	assert.Equal(t, []byte("-42"), encodedInt)
	decodedInt, err := intCodec.Decode(encodedInt)
	require.NoError(t, err)
	assert.Equal(t, codecInt(-42), decodedInt)
}

func TestDefaultCodecRoundTripsPointersAndNil(t *testing.T) {
	codec := cache.DefaultCodec[*codecValue]()
	value := &codecValue{Name: "alice", Count: 42}
	encoded, err := codec.Encode(value)
	require.NoError(t, err)
	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, value, decoded)

	encoded, err = codec.Encode(nil)
	require.NoError(t, err)
	decoded, err = codec.Decode(encoded)
	require.NoError(t, err)
	assert.Nil(t, decoded)
}

func TestDefaultCodecRejectsUntypedDecode(t *testing.T) {
	codec := cache.DefaultCodec[any]()
	encoded, err := codec.Encode(codecValue{Name: "alice"})
	require.NoError(t, err)
	_, err = codec.Decode(encoded)
	assert.ErrorIs(t, err, cache.ErrUntypedValue)
}

type failingCodec struct{}

func (failingCodec) Encode(string) ([]byte, error) { return nil, errors.New("encode failed") }
func (failingCodec) Decode([]byte) (string, error) { return "", errors.New("decode failed") }

func TestCodecInterfaceSupportsCustomImplementations(t *testing.T) {
	var codec cache.Codec[string] = failingCodec{}
	_, err := codec.Encode("value")
	assert.ErrorContains(t, err, "encode failed")
	_, err = codec.Decode(nil)
	assert.ErrorContains(t, err, "decode failed")
}

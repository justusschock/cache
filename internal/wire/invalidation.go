package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	cache "github.com/justusschock/go-cache"
)

const invalidationHeaderSize = 14

// EncodeInvalidation encodes an invalidation event for a remote transport.
func EncodeInvalidation[K comparable](event cache.InvalidationEvent[K], codec cache.Codec[K]) ([]byte, error) {
	if event.Kind != cache.InvalidateKey && event.Kind != cache.InvalidateNamespace {
		return nil, errors.New("cache wire: invalid invalidation kind")
	}
	if len(event.Namespace) > math.MaxUint32 {
		return nil, errors.New("cache wire: invalidation namespace is too large")
	}
	var key []byte
	var err error
	if event.Kind == cache.InvalidateKey {
		key, err = codec.Encode(event.Key)
		if err != nil {
			return nil, fmt.Errorf("cache wire encode invalidation key: %w", err)
		}
	}
	encoded := make([]byte, invalidationHeaderSize+len(event.Namespace)+len(key))
	encoded[0] = 1
	encoded[1] = byte(event.Kind)
	binary.BigEndian.PutUint64(encoded[2:10], event.Generation)
	binary.BigEndian.PutUint32(encoded[10:14], uint32(len(event.Namespace)))
	copy(encoded[invalidationHeaderSize:], event.Namespace)
	copy(encoded[invalidationHeaderSize+len(event.Namespace):], key)
	return encoded, nil
}

// DecodeInvalidation decodes a remote invalidation event.
func DecodeInvalidation[K comparable](encoded []byte, codec cache.Codec[K]) (cache.InvalidationEvent[K], error) {
	var zero cache.InvalidationEvent[K]
	if len(encoded) < invalidationHeaderSize || encoded[0] != 1 {
		return zero, errors.New("cache wire: malformed invalidation")
	}
	kind := cache.InvalidationKind(encoded[1])
	if kind != cache.InvalidateKey && kind != cache.InvalidateNamespace {
		return zero, errors.New("cache wire: invalid invalidation kind")
	}
	namespaceLength := int(binary.BigEndian.Uint32(encoded[10:14]))
	if namespaceLength > len(encoded)-invalidationHeaderSize {
		return zero, errors.New("cache wire: malformed invalidation namespace")
	}
	event := cache.InvalidationEvent[K]{
		Namespace:  string(encoded[invalidationHeaderSize : invalidationHeaderSize+namespaceLength]),
		Kind:       kind,
		Generation: binary.BigEndian.Uint64(encoded[2:10]),
	}
	key := encoded[invalidationHeaderSize+namespaceLength:]
	if kind == cache.InvalidateNamespace {
		if len(key) != 0 {
			return zero, errors.New("cache wire: namespace invalidation contains a key")
		}
		return event, nil
	}
	decodedKey, err := codec.Decode(key)
	if err != nil {
		return zero, fmt.Errorf("cache wire decode invalidation key: %w", err)
	}
	event.Key = decodedKey
	return event, nil
}

package wire

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"time"

	cache "github.com/justusschock/go-cache"
)

var ErrInvalidEnvelope = errors.New("cache: invalid entry envelope")

const (
	version    byte = 1
	headerSize      = 18
	flagFound  byte = 1 << 0
	flagNil    byte = 1 << 1
)

// Encoder canonicalizes keys and serializes cache entries.
type Encoder[K comparable, V any] struct {
	keyCodec   cache.Codec[K]
	valueCodec cache.Codec[V]
}

func New[K comparable, V any](keyCodec cache.Codec[K], valueCodec cache.Codec[V]) *Encoder[K, V] {
	return &Encoder[K, V]{keyCodec: keyCodec, valueCodec: valueCodec}
}

func (e *Encoder[K, V]) Key(key K) ([sha256.Size]byte, error) {
	encoded, err := e.keyCodec.Encode(key)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("cache key encode: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func Namespace(namespace string) [sha256.Size]byte {
	return sha256.Sum256([]byte(namespace))
}

func (e *Encoder[K, V]) Encode(entry cache.Entry[V], generation uint64) ([]byte, error) {
	flags := byte(0)
	if entry.Found {
		flags |= flagFound
	}
	isNil := nilValue(entry.Value)
	if isNil {
		flags |= flagNil
	}
	var value []byte
	var err error
	if !isNil {
		value, err = e.valueCodec.Encode(entry.Value)
		if err != nil {
			return nil, fmt.Errorf("cache value encode: %w", err)
		}
	}
	envelope := make([]byte, headerSize+len(value))
	envelope[0] = version
	binary.BigEndian.PutUint64(envelope[1:9], generation)
	binary.BigEndian.PutUint64(envelope[9:17], uint64(entry.ExpiresAt.UnixNano()))
	envelope[17] = flags
	copy(envelope[headerSize:], value)
	return envelope, nil
}

func (e *Encoder[K, V]) Decode(envelope []byte) (cache.Entry[V], uint64, error) {
	var zero cache.Entry[V]
	if len(envelope) < headerSize || envelope[0] != version {
		return zero, 0, ErrInvalidEnvelope
	}
	generation := binary.BigEndian.Uint64(envelope[1:9])
	expiresAt := time.Unix(0, int64(binary.BigEndian.Uint64(envelope[9:17])))
	flags := envelope[17]
	entry := cache.Entry[V]{Found: flags&flagFound != 0, ExpiresAt: expiresAt}
	if flags&flagNil == 0 {
		value, err := e.valueCodec.Decode(envelope[headerSize:])
		if err != nil {
			return zero, 0, fmt.Errorf("cache value decode: %w", err)
		}
		entry.Value = value
	}
	return entry, generation, nil
}

func nilValue[T any](value T) bool {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return true
	}
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

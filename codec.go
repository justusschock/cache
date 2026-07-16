package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

var ErrUntypedValue = errors.New("cache: decoding an interface value requires a custom codec")

// Codec converts a typed value to and from bytes for persistent stores.
type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

type automaticCodec[T any] struct{}

// DefaultCodec returns the automatic codec for T. Strings, byte slices, and
// scalar values use direct encodings; composite values use JSON.
func DefaultCodec[T any]() Codec[T] {
	return automaticCodec[T]{}
}

func (automaticCodec[T]) Encode(value T) ([]byte, error) {
	typeOf := reflect.TypeFor[T]()
	if typeOf == nil {
		return json.Marshal(value)
	}
	reflected := reflect.ValueOf(value)
	switch typeOf.Kind() {
	case reflect.String:
		return []byte(reflected.String()), nil
	case reflect.Slice:
		if typeOf.Elem().Kind() == reflect.Uint8 {
			if reflected.IsNil() {
				return nil, nil
			}
			return append([]byte(nil), reflected.Bytes()...), nil
		}
	case reflect.Bool:
		return []byte(strconv.FormatBool(reflected.Bool())), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []byte(strconv.FormatInt(reflected.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return []byte(strconv.FormatUint(reflected.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		return []byte(strconv.FormatFloat(reflected.Float(), 'g', -1, typeOf.Bits())), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cache codec encode: %w", err)
	}
	return encoded, nil
}

func (automaticCodec[T]) Decode(encoded []byte) (T, error) {
	var zero T
	typeOf := reflect.TypeFor[T]()
	if typeOf == nil || typeOf.Kind() == reflect.Interface {
		return zero, ErrUntypedValue
	}
	value := reflect.New(typeOf).Elem()
	switch typeOf.Kind() {
	case reflect.String:
		value.SetString(string(encoded))
		return value.Interface().(T), nil
	case reflect.Slice:
		if typeOf.Elem().Kind() == reflect.Uint8 {
			decoded := reflect.MakeSlice(typeOf, len(encoded), len(encoded))
			reflect.Copy(decoded, reflect.ValueOf(encoded))
			return decoded.Interface().(T), nil
		}
	case reflect.Bool:
		decoded, err := strconv.ParseBool(string(encoded))
		if err != nil {
			return zero, fmt.Errorf("cache codec decode bool: %w", err)
		}
		value.SetBool(decoded)
		return value.Interface().(T), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		decoded, err := strconv.ParseInt(string(encoded), 10, typeOf.Bits())
		if err != nil {
			return zero, fmt.Errorf("cache codec decode int: %w", err)
		}
		value.SetInt(decoded)
		return value.Interface().(T), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		decoded, err := strconv.ParseUint(string(encoded), 10, typeOf.Bits())
		if err != nil {
			return zero, fmt.Errorf("cache codec decode uint: %w", err)
		}
		value.SetUint(decoded)
		return value.Interface().(T), nil
	case reflect.Float32, reflect.Float64:
		decoded, err := strconv.ParseFloat(string(encoded), typeOf.Bits())
		if err != nil {
			return zero, fmt.Errorf("cache codec decode float: %w", err)
		}
		value.SetFloat(decoded)
		return value.Interface().(T), nil
	}
	if err := json.Unmarshal(encoded, &zero); err != nil {
		return zero, fmt.Errorf("cache codec decode: %w", err)
	}
	return zero, nil
}

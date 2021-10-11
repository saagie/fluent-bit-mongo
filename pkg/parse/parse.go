package parse

import (
	"fmt"
	"reflect"

	"github.com/saagie/fluent-bit-mongo/pkg/convert"
	"github.com/spaolacci/murmur3"
)

func GetHashesFromBytes(data []byte) ([]byte, []byte, error) {
	var seed uint32 = 42
	hashUint64 := murmur3.Sum64WithSeed(data, seed)
	hashUint32 := murmur3.Sum32WithSeed(data, seed)
	h64bytes, err := convert.UInt64ToBytes(hashUint64)
	if err != nil {
		return nil, nil, err
	}
	h32bytes, err := convert.UInt32ToBytes(hashUint32)
	if err != nil {
		return nil, nil, err
	}
	return h64bytes, h32bytes, nil
}

func KeyNotFound(key string, record map[interface{}]interface{}) error {
	keys := make([]interface{}, 0, len(record))
	for k := range record {
		keys = append(keys, k)
	}

	return &ErrKeyNotFound{LookingFor: key, found: keys}
}

type ErrKeyNotFound struct {
	LookingFor string
	found      []interface{}
}

func (err *ErrKeyNotFound) Error() string {
	if err.found == nil {
		return fmt.Sprintf("key %s not found", err.LookingFor)
	}

	return fmt.Sprintf("key %s not found in %v", err.LookingFor, err.found)
}

func (err *ErrKeyNotFound) Is(err2 error) bool {
	if err2, ok := err2.(*ErrKeyNotFound); ok {
		return err.LookingFor == err2.LookingFor
	}

	return false
}

type ErrValueType struct {
	Type         reflect.Type
	ExpectedType reflect.Type
}

func (err *ErrValueType) Error() string {
	return fmt.Sprintf("expected %s got %s", err.ExpectedType.Name(), err.Type.Name())
}

func ExtractStringValue(m map[interface{}]interface{}, k string) (string, error) {
	value, ok := m[k]
	if !ok {
		return "", KeyNotFound(k, m)
	}

	valueBytes, ok := value.([]uint8)
	if !ok {
		return "", &ErrValueType{reflect.TypeOf(value), reflect.TypeOf(valueBytes)}
	}

	return string(valueBytes), nil
}

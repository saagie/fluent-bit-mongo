package parse

import (
	"errors"
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

var ErrKeyNotFound = errors.New("key not found")

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
		return "", ErrKeyNotFound
	}

	valueBytes, ok := value.([]uint8)
	if !ok {
		return "", &ErrValueType{reflect.TypeOf(value), reflect.TypeOf(valueBytes)}
	}

	return string(valueBytes), nil
}

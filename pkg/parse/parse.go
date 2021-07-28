package parse

import (
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

func ExtractStringValue(m map[interface{}]interface{}, k string) string {
	return string(m[k].([]uint8))
}

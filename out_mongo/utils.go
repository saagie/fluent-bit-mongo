package main

import (
	"bytes"
	"encoding/binary"
	"github.com/spaolacci/murmur3"
)

func uint64ToBytes(i uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func uint32ToBytes(i uint32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func getHashesFromBytes(data []byte) ([]byte, []byte, error) {
	var seed uint32 = 42
	hashUint64 := murmur3.Sum64WithSeed(data, seed)
	hashUint32 := murmur3.Sum32WithSeed(data, seed)
	h64bytes, err := uint64ToBytes(hashUint64)
	if err != nil {
		return nil, nil, err
	}
	h32bytes, err := uint32ToBytes(hashUint32)
	if err != nil {
		return nil, nil, err
	}
	return h64bytes, h32bytes, nil
}

func extractStringValue(m map[interface{}]interface{}, k string) string {
	return string(m[k].([]uint8))
}

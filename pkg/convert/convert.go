package convert

import (
	"bytes"
	"encoding/binary"
)

func UInt64ToBytes(i uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UInt32ToBytes(i uint32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

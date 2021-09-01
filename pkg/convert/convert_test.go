package convert_test

import (
	"math"

	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/saagie/fluent-bit-mongo/pkg/convert"
)

var _ = DescribeTable("Convert uint32 number", func(value uint32, expected ...uint8) {
	expectedUint := make([]uint8, 0, 64)

	for i := len(expected); i < 4; i++ {
		expectedUint = append(expectedUint, 0)
	}

	expectedUint = append(expectedUint, expected...)

	result, err := convert.UInt32ToBytes(value)
	Expect(err).ToNot(HaveOccurred())
	Expect(result).To(BeEquivalentTo(expectedUint))
},
	Entry("zero", uint32(0), byte(0)),
	Entry("3", uint32(3), byte(3)),
	Entry("maxuint32", uint32(math.MaxUint32), byte(255), byte(255), byte(255), byte(255)),
)

var _ = DescribeTable("Convert uint64 number", func(value uint64, expected ...uint8) {
	expectedUint := make([]uint8, 0, 64)

	for i := len(expected); i < 8; i++ {
		expectedUint = append(expectedUint, 0)
	}

	expectedUint = append(expectedUint, expected...)

	result, err := convert.UInt64ToBytes(value)
	Expect(err).ToNot(HaveOccurred())
	Expect(result).To(BeEquivalentTo(expectedUint))
},
	Entry("zero", uint64(0), byte(0)),
	Entry("3", uint64(3), byte(3)),
	Entry("maxuint32", uint64(math.MaxUint32), byte(255), byte(255), byte(255), byte(255)),
	Entry("maxuint64", uint64(math.MaxUint64), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255)),
)

package parse_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/saagie/fluent-bit-mongo/pkg/parse"
)

var _ = Describe("Extract string value", func() {
	const key = "the-key"

	Context("From nil structure", func() {
		It("Should fail", func() {
			_, err := parse.ExtractStringValue(nil, key)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("From an empty structure", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{}
		})

		It("Should fail", func() {
			_, err := parse.ExtractStringValue(entry, key)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("From a structure", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{}
		})

		DescribeTable("A value", func(value interface{}, ok bool) {
			// TODO BeforeEach ?
			entry[key] = value

			result, err := parse.ExtractStringValue(entry, key)
			if ok {
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeEquivalentTo(value))
			} else {
				Expect(err).To(HaveOccurred())
			}
		},
			Entry("nil", nil, false),
			Entry("empty", "", false),
			Entry("string", []uint8("a-string"), true),
			Entry("integer", 94170, false),
		)
	})
})

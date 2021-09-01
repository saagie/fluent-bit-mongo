package entry_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/saagie/fluent-bit-mongo/pkg/entry"
)

var _ = Describe("ErrRetry", func() {
	Describe("Comparaison", func() {
		It("should not be equal to same typed error", func() {
			Expect(&entry.ErrRetry{}).ToNot(BeIdenticalTo(&entry.ErrRetry{}))
			Expect(&entry.ErrRetry{}).ToNot(Equal(&entry.ErrRetry{
				Cause: errors.New("root cause"),
			}))
			Expect(&entry.ErrRetry{}).To(WithTransform(func(err error) bool {
				return errors.Is(err, &entry.ErrRetry{})
			}, BeTrue()))
		})
	})
})

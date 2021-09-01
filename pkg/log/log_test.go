package log_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/saagie/fluent-bit-mongo/pkg/log"
)

var _ = Describe("Log creation", func() {
	Context("With a valid name", func() {
		const name = "test"

		DescribeTable("Plugin type", func(pluginType log.PluginType) {
			logger, err := log.New(pluginType, name)
			Expect(err).ToNot(HaveOccurred())
			Expect(logger).ToNot(BeNil())
		},
			Entry("filter plugin", log.FilterPlugin),
			Entry("input plugin", log.InputPlugin),
			Entry("output plugin", log.OutputPlugin),
		)
	})

	Context("With an empty name", func() {
		It("Should fail", func() {
			_, err := log.New(log.OutputPlugin, "")
			Expect(err).To(HaveOccurred())
		})
	})
})

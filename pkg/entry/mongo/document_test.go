package mongo_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/saagie/fluent-bit-mongo/pkg/entry/mongo"
	"github.com/saagie/fluent-bit-mongo/pkg/log"
)

func stringEntry(value string) []uint8 {
	return []uint8(value)
}

func timeEntry(value time.Time) []uint8 {
	v, err := value.MarshalText()
	Expect(err).ToNot(HaveOccurred())

	return []uint8(v)
}

var _ = Describe("Convert document", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.TODO()

		logger, err := log.New(log.OutputPlugin, "test")
		Expect(err).ToNot(HaveOccurred())

		ctx = log.WithLogger(ctx, logger)
	})

	Describe("Job with all fields", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{
				mongo.LogKey:            stringEntry("log"),
				mongo.StreamKey:         stringEntry("stream"),
				mongo.TimeKey:           timeEntry(time.Now()),
				mongo.JobExecutionIDKey: stringEntry("jobExecutionID"),
				mongo.ProjectIDKey:      stringEntry("projectID"),
				mongo.CustomerKey:       stringEntry("customer"),
				mongo.PlatformIDKey:     stringEntry("platformID"),
			}
		})

		It("Should work", func() {
			d, err := mongo.Convert(ctx, time.Now(), entry)
			Expect(err).ToNot(HaveOccurred())
			Expect(d).ToNot(BeNil())
			Expect(d).To(BeAssignableToTypeOf(&mongo.JobLogDocument{}))
			document := d.(*mongo.JobLogDocument)
			Expect(document.JobExecutionId).To(BeEquivalentTo(stringEntry("jobExecutionID")))
			Expect(document.Customer).To(BeEquivalentTo(entry[mongo.CustomerKey]))
		})
	})

	Describe("App with all fields", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{
				mongo.LogKey:            stringEntry("log"),
				mongo.StreamKey:         stringEntry("stream"),
				mongo.TimeKey:           timeEntry(time.Now()),
				mongo.AppIDKey:          stringEntry("appID"),
				mongo.AppExecutionIDKey: stringEntry("appExecutionID"),
				mongo.ContainerIDKey:    stringEntry("containerID"),
				mongo.ProjectIDKey:      stringEntry("projectID"),
				mongo.CustomerKey:       stringEntry("customer"),
				mongo.PlatformIDKey:     stringEntry("platformID"),
			}
		})

		It("Should work", func() {
			d, err := mongo.Convert(ctx, time.Now(), entry)
			Expect(err).ToNot(HaveOccurred())
			Expect(d).ToNot(BeNil())
			Expect(d).To(BeAssignableToTypeOf(&mongo.AppLogDocument{}))
			document := d.(*mongo.AppLogDocument)
			Expect(document.AppExecutionId).To(BeEquivalentTo(stringEntry("appExecutionID")))
			Expect(document.ContainerId).To(BeEquivalentTo(stringEntry("containerID")))
			Expect(document.Customer).To(BeEquivalentTo(entry[mongo.CustomerKey]))
		})
	})

	Context("With missing field", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{
				mongo.LogKey:            stringEntry("log"),
				mongo.StreamKey:         stringEntry("stream"),
				mongo.TimeKey:           timeEntry(time.Now()),
				mongo.JobExecutionIDKey: stringEntry("jobExecutionID"),
				mongo.ProjectIDKey:      stringEntry("projectID"),
				mongo.CustomerKey:       stringEntry("customer"),
				mongo.PlatformIDKey:     stringEntry("platformID"),
			}
		})

		DescribeTable("Field", func(field string, ok bool) {
			delete(entry, field)

			d, err := mongo.Convert(ctx, time.Now(), entry)

			if ok {
				Expect(err).ToNot(HaveOccurred())
				Expect(d).ToNot(BeNil())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},
			Entry("log message", mongo.LogKey, true),
			Entry("stream", mongo.StreamKey, true),
			Entry("time", mongo.TimeKey, true),
			Entry("job ID", mongo.JobExecutionIDKey, false),
			Entry("project ID", mongo.ProjectIDKey, false),
			Entry("customer", mongo.CustomerKey, false),
			Entry("platform ID", mongo.PlatformIDKey, false),
		)
	})

	Context("With \\n end of logs", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{
				mongo.StreamKey:         stringEntry("stream"),
				mongo.TimeKey:           timeEntry(time.Now()),
				mongo.AppIDKey:          stringEntry("appID"),
				mongo.AppExecutionIDKey: stringEntry("appExecutionID"),
				mongo.ContainerIDKey:    stringEntry("containerID"),
				mongo.ProjectIDKey:      stringEntry("projectID"),
				mongo.CustomerKey:       stringEntry("customer"),
				mongo.PlatformIDKey:     stringEntry("platformID"),
			}
		})

		It("Should remove \\n from log end", func() {
			entry[mongo.LogKey] = stringEntry("log\n")

			d, err := mongo.Convert(ctx, time.Now(), entry)
			Expect(err).ToNot(HaveOccurred())
			Expect(d).ToNot(BeNil())
			Expect(d).To(BeAssignableToTypeOf(&mongo.AppLogDocument{}))
			document := d.(*mongo.AppLogDocument)
			Expect(document.Log).To(BeEquivalentTo(stringEntry("log")))
		})

		It("Should not change content log without \\n", func() {
			entry[mongo.LogKey] = stringEntry("log")

			d, err := mongo.Convert(ctx, time.Now(), entry)
			Expect(err).ToNot(HaveOccurred())
			Expect(d).ToNot(BeNil())
			Expect(d).To(BeAssignableToTypeOf(&mongo.AppLogDocument{}))
			document := d.(*mongo.AppLogDocument)
			Expect(document.Log).To(BeEquivalentTo(stringEntry("log")))
		})
	})
})

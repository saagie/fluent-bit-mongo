package log_test

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/saagie/fluent-bit-mongo/pkg/log"
)

var _ = Describe("Add a valid logger", func() {
	var logger log.Logger
	var ctx context.Context

	BeforeEach(func() {
		l, err := log.New(log.OutputPlugin, "test")
		Expect(err).ToNot(HaveOccurred())

		logger = l
	})

	Context("To nil context", func() {
		It("Should fail", func() {
			Expect(func() {
				log.WithLogger(ctx, logger)
			}).To(Panic())

		})
	})

	Context("To a valid context", func() {
		BeforeEach(func() {
			ctx = context.TODO()
		})

		It("Should work", func() {
			ctx := log.WithLogger(ctx, logger)
			Expect(ctx).ToNot(BeNil())

			newLogger, err := log.GetLogger(ctx)
			Expect(err).ToNot(HaveOccurred())

			Expect(newLogger).To(Equal(logger))
		})

		Context("Already containing a logger", func() {
			BeforeEach(func() {
				l, err := log.New(log.OutputPlugin, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(l).NotTo(BeEquivalentTo(logger))

				ctx = log.WithLogger(ctx, l)
				Expect(ctx).ToNot(BeNil())
			})

			It("Should override the logger", func() {
				ctx := log.WithLogger(ctx, logger)
				Expect(ctx).ToNot(BeNil())

				newLogger, err := log.GetLogger(ctx)
				Expect(err).ToNot(HaveOccurred())

				Expect(newLogger).To(Equal(logger))
			})
		})
	})
})

var _ = Describe("Add an empty logger", func() {
	var logger log.Logger
	var ctx context.Context

	Context("To nil context", func() {
		It("Should fail", func() {
			Expect(func() {
				log.WithLogger(ctx, logger)
			}).To(Panic())

		})
	})

	Context("To a valid context", func() {
		BeforeEach(func() {
			ctx = context.TODO()
		})

		It("Should fail", func() {
			ctx := log.WithLogger(ctx, logger)
			Expect(ctx).ToNot(BeNil())

			_, err := log.GetLogger(ctx)
			Expect(err).To(MatchError(log.ErrNoLoggerFound))
		})

		Context("Already containing a logger", func() {
			BeforeEach(func() {
				l, err := log.New(log.OutputPlugin, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(l).NotTo(BeEquivalentTo(logger))

				ctx = log.WithLogger(ctx, l)
				Expect(ctx).ToNot(BeNil())
			})

			It("Should override the logger", func() {
				ctx := log.WithLogger(ctx, logger)
				Expect(ctx).ToNot(BeNil())

				_, err := log.GetLogger(ctx)
				Expect(err).To(MatchError(log.ErrNoLoggerFound))
			})
		})
	})
})

package main_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func TestFluentBitMongo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FluentBitMongo Suite")
}

var dockerPool *dockertest.Pool

const imageName = "docker.io/saagie/fluent-bit-mongo"
const imageTag = "test"

const RunIDKey = "test-id"

func runID() string {
	return fmt.Sprintf("%d-%d", GinkgoRandomSeed(), GinkgoParallelNode())
}

var _ = BeforeSuite(func() {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	Expect(err).ToNot(HaveOccurred())

	dockerPool = pool

	info, err := dockerPool.Client.Info()
	Expect(err).ToNot(HaveOccurred())

	fmt.Fprintf(GinkgoWriter, "Using %s (%s)\n", info.Name, info.ServerVersion)
	fmt.Fprint(GinkgoWriter, "Building image ... ")

	Expect(dockerPool.Client.BuildImage(docker.BuildImageOptions{
		Name:         fmt.Sprintf("%s:%s", imageName, imageTag),
		Dockerfile:   "./Dockerfile",
		OutputStream: GinkgoWriter,
		ErrorStream:  GinkgoWriter,
		ContextDir:   ".",
		Labels: map[string]string{
			RunIDKey:             runID(),
			"ginkgo-description": CurrentGinkgoTestDescription().TestText,
			"ginkgo-seed":        strconv.FormatInt(GinkgoRandomSeed(), 10),
		},
		SuppressOutput: true,
	})).To(Succeed())
})

var _ = AfterSuite(func() {
	containers, err := dockerPool.Client.ListContainers(docker.ListContainersOptions{})
	Expect(err).ToNot(HaveOccurred())

	var wg sync.WaitGroup

	for _, container := range containers {
		if container.Labels[RunIDKey] != runID() {
			continue
		}

		wg.Add(1)
		go func(container docker.APIContainers) {
			defer wg.Done()
			defer GinkgoRecover()

			if res, ok := dockerPool.ContainerByName(container.ID); ok {
				Expect(dockerPool.Purge(res)).To(Succeed())
			}
		}(container)
	}

	wg.Wait()
})

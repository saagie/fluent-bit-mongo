package main_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type FluentBitHome struct {
	FluentBit struct {
		Version string
		Edition string
		Flags   []string
	} `json:"fluent-bit"`
}

type FluentBitMetrics struct {
	Input  map[string]map[string]int
	Filter map[string]map[string]int
	Output map[string]map[string]int
}

const (
	inputName  = "tail.0"  // It depends of the order of the input in the fluent-bit configuration
	outputName = "mongo.1" // It depends of the order of the output in the fluent-bit configuration

	logFileCount  = 1
	logEntryCount = 1
)

const (
	mongoUser         = "root"
	mongoPassword     = "password"
	mongoAuthDatabase = "admin"
	mongoDatabase     = "fluent_bit"
)

var _ = Describe("Run fluent-bit", func() {
	var fluentBitRunOptions *dockertest.RunOptions
	var fluentBit *dockertest.Resource
	var fluentBitURL string

	const apiPortName = "2020/tcp"

	BeforeEach(func() {
		cwd, err := os.Getwd()
		Expect(err).ToNot(HaveOccurred())

		logPath := fmt.Sprintf("/var/log/%s.log", runID())

		fluentBitRunOptions = &dockertest.RunOptions{
			Repository:   imageName,
			Tag:          imageTag,
			ExposedPorts: []string{apiPortName},
			Labels: map[string]string{
				RunIDKey: runID(),
			},
			PortBindings: map[docker.Port][]docker.PortBinding{
				docker.Port(apiPortName): {{
					HostIP: "localhost",
				}},
			},
			Env: []string{
				fmt.Sprintf("MONGO_USERNAME=%s", mongoUser),
				fmt.Sprintf("MONGO_PASSWORD=%s", mongoPassword),
				fmt.Sprintf("MONGO_AUTH_DATABASE=%s", mongoAuthDatabase),
				fmt.Sprintf("MONGO_DATABASE=%s", mongoDatabase),
				fmt.Sprintf("LOG_FILE=%s", logPath),
			},
			Mounts: []string{
				fmt.Sprintf("%s:%s:ro", path.Join(cwd, "tests/fluent-bit.conf"), "/fluent-bit/etc/fluent-bit.conf"),
				fmt.Sprintf("%s:%s:ro", path.Join(cwd, "tests/test.json"), logPath),
			},
		}
	})

	JustBeforeEach(func() {
		resource, err := dockerPool.RunWithOptions(fluentBitRunOptions, func(config *docker.HostConfig) {
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		})
		Expect(err).ToNot(HaveOccurred())

		fluentBitURL = fmt.Sprintf("http://%s:%s", "localhost", resource.GetPort(apiPortName))
		fluentBit = resource

		By("Checking fluent-bit endpoint availability", func() {
			var response *http.Response

			Eventually(func() (err error) {
				response, err = http.Get(fluentBitURL)

				return err
			}, "10s").Should(Succeed())
			Expect(response).To(HaveHTTPStatus(http.StatusOK))

			body, err := ioutil.ReadAll(response.Body)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.Body.Close()).To(Succeed())

			var fluentBitHome FluentBitHome
			Expect(json.Unmarshal(body, &fluentBitHome)).To(Succeed())

			fmt.Fprintf(GinkgoWriter, "fluentbit %s (%s) started\n", fluentBitHome.FluentBit.Edition, fluentBitHome.FluentBit.Version)
		})

		By("Checking fluent-bit input", func() {
			Eventually(func() *http.Response {
				response, err := http.Get(fmt.Sprintf("%s/api/v1/metrics", fluentBitURL))
				Expect(err).ToNot(HaveOccurred())

				return response
			}, "3s").Should(HaveHTTPStatus(http.StatusOK))

			Eventually(func() int {
				response, err := http.Get(fmt.Sprintf("%s/api/v1/metrics", fluentBitURL))
				Expect(err).ToNot(HaveOccurred())
				Expect(response).To(HaveHTTPStatus(http.StatusOK))

				body, err := ioutil.ReadAll(response.Body)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.Body.Close()).To(Succeed())

				var result FluentBitMetrics

				Expect(json.Unmarshal(body, &result)).To(Succeed())

				inputMetrics, ok := result.Input[inputName]
				Expect(ok).To(BeTrue())

				fmt.Fprintf(GinkgoWriter, "fluent-bit metrics: %s: %+v\n", inputName, inputMetrics)

				return inputMetrics["files_opened"]
			}).Should(Equal(logFileCount))
		})
	})

	AfterEach(func() {
		if fluentBit == nil {
			return
		}

		Expect(fluentBit.Close()).To(Succeed())
	})

	JustAfterEach(func() {
		if fluentBit == nil {
			return
		}

		if CurrentGinkgoTestDescription().Failed {
			By("Reading fluent-bit logs", func() {
				defer GinkgoRecover()

				fmt.Fprintln(GinkgoWriter, "==== FluentBit logs ====")

				err := dockerPool.Client.Logs(docker.LogsOptions{
					Container:    fluentBit.Container.ID,
					OutputStream: GinkgoWriter,
					ErrorStream:  GinkgoWriter,
					Stdout:       true,
					Stderr:       true,
					Timestamps:   false,
					Follow:       false,
				})
				defer Expect(err).ToNot(HaveOccurred())

				fmt.Fprintln(GinkgoWriter, "====   Ends logs    ====")
			})
		}

		By("Killing fluent-bit", func() {
			Expect(dockerPool.Client.KillContainer(docker.KillContainerOptions{
				ID:     fluentBit.Container.ID,
				Signal: docker.SIGINT,
			})).To(Succeed())

			const waitForStop = 6 * time.Second // Fluent-bit service will stop in 5 seconds

			Eventually(func() bool {
				container, ok := dockerPool.ContainerByName(fluentBit.Container.ID)

				return ok && container.Container.State.Running
			}, waitForStop).Should(BeFalse())
		})
	})

	Context("With running mongoDB", func() {
		const mongoTag = "4.2.12" // https://hub.docker.com/_/mongo?tab=tags

		var mongoDB *dockertest.Resource

		BeforeEach(func() {
			By("Starting mongodb", func() {
				// https://hub.docker.com/_/mongo
				resource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
					Repository:   "mongo",
					Tag:          mongoTag,
					Hostname:     "mongo",
					ExposedPorts: []string{"27017/tcp"},
					Labels: map[string]string{
						RunIDKey: runID(),
					},
					Env: []string{
						fmt.Sprintf("MONGO_INITDB_ROOT_USERNAME=%s", mongoUser),
						fmt.Sprintf("MONGO_INITDB_ROOT_PASSWORD=%s", mongoPassword),
					},
				})
				Expect(err).ToNot(HaveOccurred())

				mongoDB = resource

				fluentBitRunOptions.Links = append(fluentBitRunOptions.Links, fmt.Sprintf("%s:%s", resource.Container.ID, resource.Container.Config.Hostname))
			})
		})

		AfterEach(func() {
			if mongoDB == nil {
				return
			}

			if _, ok := os.LookupEnv("DEBUG"); ok && CurrentGinkgoTestDescription().Failed {
				By("Reading mongodb logs", func() {
					fmt.Fprintln(GinkgoWriter, "==== MongoDB logs ====")

					err := dockerPool.Client.Logs(docker.LogsOptions{
						Container:    mongoDB.Container.ID,
						OutputStream: GinkgoWriter,
						ErrorStream:  GinkgoWriter,
						Stdout:       true,
						Stderr:       true,
						Timestamps:   false,
						Follow:       false,
					})
					defer Expect(err).ToNot(HaveOccurred())

					fmt.Fprintln(GinkgoWriter, "====  Ends logs   ====")
				})
			}

			Expect(mongoDB.Close()).To(Succeed())
		})

		It("Should work", func() {
			By("Checking entry count", func() {
				Eventually(func() int {
					response, err := http.Get(fmt.Sprintf("%s/api/v1/metrics", fluentBitURL))
					Expect(err).ToNot(HaveOccurred())
					Expect(response).To(HaveHTTPStatus(http.StatusOK))

					body, err := ioutil.ReadAll(response.Body)
					Expect(err).ToNot(HaveOccurred())

					Expect(response.Body.Close()).To(Succeed())

					var result FluentBitMetrics

					Expect(json.Unmarshal(body, &result)).To(Succeed())

					outputMetrics, ok := result.Output[outputName]
					Expect(ok).To(BeTrue())

					fmt.Fprintf(GinkgoWriter, "fluent-bit metrics: %s: %+v\n", outputName, outputMetrics)

					processed, ok := outputMetrics["proc_records"]
					Expect(ok).To(BeTrue(), "proc_records not found for output %s", outputName)

					retried := outputMetrics["retried_records"]
					dropped := outputMetrics["dropped_records"]

					return processed + retried + dropped
				}, "10s").Should(Equal(logEntryCount))
			})

			By("Checking entry state", func() {
				response, err := http.Get(fmt.Sprintf("%s/api/v1/metrics", fluentBitURL))
				Expect(err).ToNot(HaveOccurred())

				body, err := ioutil.ReadAll(response.Body)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.Body.Close()).To(Succeed())

				var result FluentBitMetrics
				Expect(json.Unmarshal(body, &result)).To(Succeed())

				Expect(result.Output[outputName]["errors"]).To(BeZero())
				Expect(result.Output[outputName]["proc_records"]).To(Equal(logEntryCount))
			})
		})
	})
})

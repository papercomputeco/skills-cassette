package server_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
)

var _ = Describe("health server", func() {
	It("serves GET /ping and leaves product routes dark", func() {
		recorder := httptest.NewRecorder()
		server.New().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ping", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.String()).To(Equal("pong\n"))

		recorder = httptest.NewRecorder()
		server.New().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/skills", nil))
		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})

	It("shuts down when its context is canceled", func() {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- server.Serve(ctx, listener) }()

		response, err := http.Get("http://" + listener.Addr().String() + "/ping")
		Expect(err).NotTo(HaveOccurred())
		_, _ = io.Copy(io.Discard, response.Body)
		Expect(response.Body.Close()).To(Succeed())
		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})
})

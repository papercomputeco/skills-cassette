package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

func newTestServer() *server.Server {
	return server.New(server.Config{}, storage.NewMemoryStore(), nil, nil)
}

var _ = Describe("cassette anchors", func() {
	It("serves GET /ping", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ping", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.String()).To(Equal("pong\n"))
	})

	It("serves nothing at the legacy core route", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/skills", nil))
		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})

	It("serves an OpenAPI document that satisfies cassette admission", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Header().Get("Content-Type")).To(Equal("application/json"))

		var document map[string]any
		Expect(json.Unmarshal(recorder.Body.Bytes(), &document)).To(Succeed())

		// The manifest core admits the cassette on rides in the document.
		manifest, ok := document["x-tapes-cassette"].(map[string]any)
		Expect(ok).To(BeTrue(), "x-tapes-cassette root extension is required for admission")
		Expect(manifest).To(HaveKeyWithValue("kind", "cassette/v1alpha1"))
		identity, _ := manifest["cassette"].(map[string]any)
		Expect(identity).To(HaveKeyWithValue("name", "skills"))
		anchors, _ := manifest["api"].(map[string]any)
		Expect(anchors).To(HaveKeyWithValue("prefix_path", "api"))

		// Every declared path must be contained by /api/skills — a path outside
		// the prefix fails the whole document at admission.
		paths, _ := document["paths"].(map[string]any)
		Expect(paths).NotTo(BeEmpty())
		seenOperationIDs := map[string]bool{}
		for path, item := range paths {
			Expect(path == "/api/skills" || strings.HasPrefix(path, "/api/skills/")).
				To(BeTrue(), "path %q escapes the declared prefix", path)
			for method, op := range item.(map[string]any) {
				if method == "parameters" {
					continue
				}
				operation, ok := op.(map[string]any)
				Expect(ok).To(BeTrue())
				// Admission requires a response on every operation and
				// unique operation ids within the cassette.
				Expect(operation).To(HaveKey("responses"), "%s %s", method, path)
				id, _ := operation["operationId"].(string)
				Expect(id).NotTo(BeEmpty(), "%s %s", method, path)
				Expect(seenOperationIDs[id]).To(BeFalse(), "duplicate operationId %q", id)
				seenOperationIDs[id] = true
			}
		}
	})

	It("documents persisted drafts and conditional writes", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		var document map[string]any
		Expect(json.Unmarshal(recorder.Body.Bytes(), &document)).To(Succeed())
		paths := document["paths"].(map[string]any)
		Expect(paths).To(HaveKey("/api/skills/drafts/generate"))
		Expect(paths).NotTo(HaveKey("/api/skills/generate"))
		Expect(paths).NotTo(HaveKey("/api/skills/revise"))

		requestSchema := func(path, method string) map[string]any {
			op := paths[path].(map[string]any)[method].(map[string]any)
			body := op["requestBody"].(map[string]any)
			content := body["content"].(map[string]any)["application/json"].(map[string]any)
			return content["schema"].(map[string]any)
		}
		Expect(requestSchema("/api/skills/{id}/draft", "put")["required"]).To(ConsistOf("revision"))
		Expect(requestSchema("/api/skills/{id}/draft/revise", "post")["required"]).To(ConsistOf("revision", "instruction"))
		Expect(requestSchema("/api/skills/{id}/publish", "post")["required"]).To(ConsistOf("revision"))
		publish := paths["/api/skills/{id}/publish"].(map[string]any)["post"].(map[string]any)
		Expect(publish["responses"]).To(HaveKey("400"))
	})

	It("republishes under an installed name other than the default", func() {
		srv := server.New(server.Config{Name: "skills-two"}, storage.NewMemoryStore(), nil, nil)
		recorder := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		var document map[string]any
		Expect(json.Unmarshal(recorder.Body.Bytes(), &document)).To(Succeed())
		paths, _ := document["paths"].(map[string]any)
		for path := range paths {
			Expect(strings.HasPrefix(path, "/api/skills-two")).To(BeTrue())
		}

		recorder = httptest.NewRecorder()
		srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/skills-two", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
	})

	It("shuts down when its context is canceled", func() {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- newTestServer().Serve(ctx, listener) }()

		response, err := http.Get("http://" + listener.Addr().String() + "/ping")
		Expect(err).NotTo(HaveOccurred())
		_, _ = io.Copy(io.Discard, response.Body)
		Expect(response.Body.Close()).To(Succeed())
		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})
})

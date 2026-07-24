package skill_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

var _ = Describe("tapes-core HTTP querier", func() {
	It("reads the real trace list wire shape and query", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.Path).To(Equal("/v1/traces"))
			Expect(r.URL.Query().Get("session_id")).To(Equal("session/one"))
			_, _ = fmt.Fprint(w, `{"items":[{"trace_id":"trace-1","user_prompt":"prompt","response_preview":"answer","synthetic":"","started_at":"2026-06-01T10:00:00Z","usage":{"input_tokens":12,"output_tokens":3},"main_usage":{"input_tokens":10,"output_tokens":2}}]}`)
		}))
		defer server.Close()
		items, err := skill.NewAPIClient(server.URL+"/").TraceSummaries(context.Background(), "session/one")
		Expect(err).NotTo(HaveOccurred())
		Expect(items).To(HaveLen(1))
		Expect(items[0].TraceID).To(Equal("trace-1"))
		Expect(items[0].TotalInputTokens).To(Equal(int64(12)))
		Expect(items[0].MainOutputTokens).To(Equal(int64(2)))
	})

	It("reads escaped trace paths and local content-block wire shapes", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.EscapedPath()).To(Equal("/v1/traces/trace%2Fone"))
			_, _ = fmt.Fprint(w, `{"trace":{"trace_id":"trace/one"},"spans":[{"span_id":"span-1","kind":"llm","seq":1,"call_kind":"main","thread_id":"","output":[{"type":"text","text":"answer"}]}]}`)
		}))
		defer server.Close()
		trace, err := skill.NewAPIClient(server.URL).Trace(context.Background(), "trace/one")
		Expect(err).NotTo(HaveOccurred())
		Expect(trace.TraceID).To(Equal("trace/one"))
		Expect(trace.Spans[0].Output[0].Text).To(Equal("answer"))
	})

	It("reports invalid JSON", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, `{`) }))
		defer server.Close()
		_, err := skill.NewAPIClient(server.URL).TraceSummaries(context.Background(), "session")
		Expect(err).To(MatchError(ContainSubstring("decoding response")))
	})

	It("rejects oversized success responses", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprint(w, strings.Repeat("x", 17<<20))
		}))
		defer server.Close()

		_, err := skill.NewAPIClient(server.URL).TraceSummaries(context.Background(), "session")
		Expect(err).To(MatchError(ContainSubstring("response body exceeds")))
	})

	It("bounds error response text", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = fmt.Fprint(w, strings.Repeat("x", 65<<10))
		}))
		defer server.Close()

		_, err := skill.NewAPIClient(server.URL).TraceSummaries(context.Background(), "session")
		Expect(err).To(MatchError(ContainSubstring("response body exceeds")))
	})
})

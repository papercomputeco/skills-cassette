package skill_test

import (
	"context"
	"errors"
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

	It("walks paged trace spans and returns the turn whole", func() {
		var cursors []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.EscapedPath()).To(Equal("/v1/traces/trace-1"))
			cursor := r.URL.Query().Get("cursor")
			cursors = append(cursors, cursor)
			switch cursor {
			case "":
				_, _ = fmt.Fprint(w, `{"trace":{"trace_id":"trace-1"},"spans":[{"span_id":"s1","kind":"llm","seq":1,"call_kind":"main","output":[{"type":"text","text":"first"}]}],"next_cursor":"c 2"}`)
			case "c 2":
				_, _ = fmt.Fprint(w, `{"trace":{"trace_id":"trace-1"},"spans":[{"span_id":"s2","kind":"tool","seq":2,"call_kind":"main"}],"next_cursor":"c3"}`)
			case "c3":
				_, _ = fmt.Fprint(w, `{"trace":{"trace_id":"trace-1"},"spans":[{"span_id":"s3","kind":"llm","seq":3,"call_kind":"main","output":[{"type":"text","text":"final answer"}]}]}`)
			}
		}))
		defer server.Close()
		trace, err := skill.NewAPIClient(server.URL).Trace(context.Background(), "trace-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(cursors).To(Equal([]string{"", "c 2", "c3"}))
		Expect(trace.Spans).To(HaveLen(3))
		Expect(trace.Spans[0].SpanID).To(Equal("s1"))
		Expect(trace.Spans[2].Output[0].Text).To(Equal("final answer"),
			"a long turn's final text lives on its last page and must not be dropped")
	})

	It("makes one request when tapes serves the turn whole", func() {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			_, _ = fmt.Fprint(w, `{"trace":{"trace_id":"trace-1"},"spans":[{"span_id":"s1","kind":"llm","seq":1,"call_kind":"main"}]}`)
		}))
		defer server.Close()
		trace, err := skill.NewAPIClient(server.URL).Trace(context.Background(), "trace-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(trace.Spans).To(HaveLen(1))
		Expect(requests).To(Equal(1))
	})

	It("fails instead of looping when a cursor repeats", func() {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			_, _ = fmt.Fprint(w, `{"trace":{"trace_id":"trace-1"},"spans":[],"next_cursor":"again"}`)
		}))
		defer server.Close()
		_, err := skill.NewAPIClient(server.URL).Trace(context.Background(), "trace-1")
		Expect(err).To(MatchError(ContainSubstring("cursor repeated")))
		Expect(requests).To(Equal(2))
	})

	It("stops walking pages once the context is cancelled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			cancel()
			_, _ = fmt.Fprintf(w, `{"trace":{"trace_id":"trace-1"},"spans":[],"next_cursor":"c%d"}`, requests)
		}))
		defer server.Close()
		_, err := skill.NewAPIClient(server.URL).Trace(ctx, "trace-1")
		Expect(errors.Is(err, context.Canceled)).To(BeTrue())
		Expect(requests).To(Equal(1))
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
		var callError *skill.ExternalCallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeFalse(), "a deterministic bounded-response overflow must not hot-retry")
	})

	It("keeps transient response read failures retryable", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			_, _ = fmt.Fprint(w, `{}`)
		}))
		defer server.Close()

		_, err := skill.NewAPIClient(server.URL).TraceSummaries(context.Background(), "session")
		Expect(err).To(MatchError(ContainSubstring("unexpected EOF")))
		var callError *skill.ExternalCallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeTrue(), "a transport read failure may succeed on a bounded retry")
	})

	It("bounds error response text", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = fmt.Fprint(w, strings.Repeat("x", 65<<10))
		}))
		defer server.Close()

		_, err := skill.NewAPIClient(server.URL).TraceSummaries(context.Background(), "session")
		Expect(err).To(MatchError(ContainSubstring("response body exceeds")))
		var callError *skill.ExternalCallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeFalse())
	})

	It("classifies Tapes API failures without parsing response text", func() {
		for _, testCase := range []struct {
			status    int
			retryable bool
			notFound  bool
		}{
			{status: http.StatusServiceUnavailable, retryable: true},
			{status: http.StatusTooManyRequests, retryable: true},
			{status: http.StatusBadRequest, retryable: false},
			{status: http.StatusNotFound, retryable: false, notFound: true},
		} {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = fmt.Fprint(w, "raw upstream detail")
			}))
			_, err := skill.NewAPIClient(server.URL).TraceSummaries(context.Background(), "session")
			server.Close()
			var callError *skill.ExternalCallError
			Expect(errors.As(err, &callError)).To(BeTrue(), "status %d", testCase.status)
			Expect(callError.Retryable).To(Equal(testCase.retryable), "status %d", testCase.status)
			Expect(skill.IsRetryableExternalError(err)).To(Equal(testCase.retryable), "status %d", testCase.status)
			Expect(errors.Is(err, skill.ErrNotFound)).To(Equal(testCase.notFound), "status %d", testCase.status)
		}
	})

	It("rejects oversized provider responses without retrying", func() {
		calls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			_, _ = fmt.Fprint(w, strings.Repeat("x", 3<<20))
		}))
		defer server.Close()

		caller, err := skill.NewLLMCaller(skill.LLMCallerConfig{
			Provider: "openai", Model: "test", APIKey: "test", BaseURL: server.URL,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = caller(context.Background(), "test prompt")
		Expect(err).To(MatchError(ContainSubstring("response body exceeds")))
		var callError *skill.ExternalCallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeFalse())
		Expect(calls).To(Equal(1), "provider response overflow is terminal")
	})

	It("retries transient provider response read failures", func() {
		calls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			if calls == 1 {
				w.Header().Set("Content-Length", "100")
				_, _ = fmt.Fprint(w, `{}`)
				return
			}
			_, _ = fmt.Fprint(w, `{"choices":[{"message":{"content":"{}"}}]}`)
		}))
		defer server.Close()

		caller, err := skill.NewLLMCaller(skill.LLMCallerConfig{
			Provider: "openai", Model: "test", APIKey: "test", BaseURL: server.URL,
		})
		Expect(err).NotTo(HaveOccurred())
		response, err := caller(context.Background(), "test prompt")
		Expect(err).NotTo(HaveOccurred())
		Expect(response).To(Equal(`{}`))
		Expect(calls).To(Equal(2))
	})

	It("classifies configured LLM provider failures", func() {
		for _, testCase := range []struct {
			status    int
			retryable bool
		}{
			{status: http.StatusServiceUnavailable, retryable: true},
			{status: http.StatusBadRequest, retryable: false},
		} {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(testCase.status)
				_, _ = fmt.Fprint(w, "raw provider detail")
			}))
			caller, err := skill.NewLLMCaller(skill.LLMCallerConfig{
				Provider: "openai", Model: "test", APIKey: "test", BaseURL: server.URL,
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = caller(context.Background(), "test prompt")
			server.Close()
			var callError *skill.ExternalCallError
			Expect(errors.As(err, &callError)).To(BeTrue(), "status %d", testCase.status)
			Expect(callError.Retryable).To(Equal(testCase.retryable), "status %d", testCase.status)
			if testCase.retryable {
				Expect(calls).To(Equal(2), "transient provider failures receive the bounded internal retry")
			} else {
				Expect(calls).To(Equal(1))
			}
		}
	})
})

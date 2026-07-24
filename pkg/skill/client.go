package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// httpClientTimeout bounds every trace-API request the generator makes.
	httpClientTimeout = 30 * time.Second

	// Trace detail includes full span payloads, so allow a bounded but generous
	// response. Error text is kept much smaller because it is only diagnostic.
	maxTraceResponseBytes = 16 << 20
	maxErrorResponseBytes = 64 << 10
)

// Querier is the read surface skill generation needs from the tapes
// trace API: turn summaries for a session, and span payloads for one
// turn. The HTTP client below implements it; tests substitute a fake.
type Querier interface {
	// TraceSummaries returns the user-visible turns of a session
	// (GET /v1/traces?session_id=), newest derive's projection.
	TraceSummaries(ctx context.Context, sessionID string) ([]TraceSummary, error)

	// Trace returns one turn's spans with full payloads
	// (GET /v1/traces/{trace_id}).
	Trace(ctx context.Context, traceID string) (*Trace, error)
}

// TraceSummary is one user-visible turn header — the turn-grain
// prompt/response pair the deriver folded for the session.
type TraceSummary struct {
	TraceID         string
	UserPrompt      string
	ResponsePreview string
	// Synthetic is non-empty for turns the harness manufactured
	// (compaction, resume replay); they carry no user intent and are
	// excluded from skill transcripts.
	Synthetic string
	StartedAt time.Time
	// Token counts folded by the deriver for the turn. Total* spans the
	// whole turn (spine + harness shadow calls); Main* counts only the
	// conversation-spine calls. Surfaced by the session export.
	TotalInputTokens  int64
	TotalOutputTokens int64
	MainInputTokens   int64
	MainOutputTokens  int64
}

// Trace is one turn's span detail.
type Trace struct {
	TraceID string
	Spans   []Span
}

// Span is the slice of the API's span shape the transcript builder
// consumes: identity, ordering, the call-kind taxonomy, and the
// decoded output content for llm spans.
type Span struct {
	SpanID       string
	ParentSpanID string
	Kind         string // "llm", "tool", "agent", "event"
	Name         string // tool name for tool spans
	Seq          int64
	// CallKind is the derive-time taxonomy ("main", "offshoot:…",
	// "injected:…") carried in span metadata; empty for tool spans.
	CallKind string
	// ThreadID is the harness sub-thread ("" = the main conversation).
	ThreadID string
	// Output is the decoded output content for llm spans (nil for
	// tool spans — the builder only needs their Name).
	Output []ContentBlock
}

// APIClient implements Querier against a running tapes API server. The
// wire shapes mirror api.TraceListResponse / api.TraceDetail without
// importing the server package — same precedent as the other CLI-side
// clients.
type APIClient struct {
	apiTarget string
	client    *http.Client
}

// NewAPIClient constructs an APIClient pointed at apiTarget (e.g.
// "http://127.0.0.1:8081").
func NewAPIClient(apiTarget string) *APIClient {
	return &APIClient{
		apiTarget: normalizeAPITarget(apiTarget),
		client:    &http.Client{Timeout: httpClientTimeout},
	}
}

var _ Querier = (*APIClient)(nil)

func normalizeAPITarget(apiTarget string) string {
	target := strings.TrimSpace(apiTarget)
	if target == "" {
		return ""
	}
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	return strings.TrimRight(target, "/")
}

// wireTrace mirrors api.TraceItem. Token totals live under `usage`; the
// conversation-spine slice under `main_usage`. Synthetic is a typed
// deriver field (was carried in the old metadata grab-bag).
type wireTrace struct {
	TraceID         string         `json:"trace_id"`
	UserPrompt      string         `json:"user_prompt"`
	ResponsePreview string         `json:"response_preview"`
	Synthetic       string         `json:"synthetic"`
	StartedAt       time.Time      `json:"started_at"`
	Usage           wireTraceUsage `json:"usage"`
	MainUsage       wireMainUsage  `json:"main_usage"`
}

// wireTraceUsage / wireMainUsage mirror api.TraceUsage / api.MainUsage —
// the subset (input/output tokens) the transcript builder surfaces.
type wireTraceUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type wireMainUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// wireSpan mirrors the subset of api.SpanItem the builder consumes. The
// harness taxonomy (call_kind, thread_id) is typed rather than bagged in a
// metadata map, and output is a content-block array uniform for every kind.
type wireSpan struct {
	SpanID       string          `json:"span_id"`
	ParentSpanID string          `json:"parent_span_id"`
	Kind         string          `json:"kind"`
	Name         string          `json:"name"`
	Seq          int64           `json:"seq"`
	CallKind     string          `json:"call_kind"`
	ThreadID     string          `json:"thread_id"`
	Output       json.RawMessage `json:"output"`
}

// wireTraceList mirrors api.TraceListResponse.
type wireTraceList struct {
	Items []wireTrace `json:"items"`
}

// wireTraceDetail mirrors api.TraceDetail.
type wireTraceDetail struct {
	Trace wireTrace  `json:"trace"`
	Spans []wireSpan `json:"spans"`
}

// TraceSummaries implements Querier via GET /v1/traces?session_id=.
func (c *APIClient) TraceSummaries(ctx context.Context, sessionID string) ([]TraceSummary, error) {
	u, err := url.Parse(c.apiTarget + "/v1/traces")
	if err != nil {
		return nil, fmt.Errorf("invalid api target: %w", err)
	}
	q := u.Query()
	q.Set("session_id", sessionID)
	u.RawQuery = q.Encode()

	var list wireTraceList
	if err := c.getJSON(ctx, u.String(), &list); err != nil {
		return nil, fmt.Errorf("list traces for session %s: %w", sessionID, err)
	}

	out := make([]TraceSummary, 0, len(list.Items))
	for _, item := range list.Items {
		out = append(out, TraceSummary{
			TraceID:           item.TraceID,
			UserPrompt:        item.UserPrompt,
			ResponsePreview:   item.ResponsePreview,
			Synthetic:         item.Synthetic,
			StartedAt:         item.StartedAt,
			TotalInputTokens:  item.Usage.InputTokens,
			TotalOutputTokens: item.Usage.OutputTokens,
			MainInputTokens:   item.MainUsage.InputTokens,
			MainOutputTokens:  item.MainUsage.OutputTokens,
		})
	}
	return out, nil
}

// Trace implements Querier via GET /v1/traces/{trace_id}.
func (c *APIClient) Trace(ctx context.Context, traceID string) (*Trace, error) {
	u := c.apiTarget + "/v1/traces/" + url.PathEscape(traceID)
	var detail wireTraceDetail
	if err := c.getJSON(ctx, u, &detail); err != nil {
		return nil, fmt.Errorf("get trace %s: %w", traceID, err)
	}

	trace := &Trace{TraceID: detail.Trace.TraceID, Spans: make([]Span, 0, len(detail.Spans))}
	for _, sp := range detail.Spans {
		span := Span{
			SpanID: sp.SpanID, ParentSpanID: sp.ParentSpanID, Kind: sp.Kind,
			Name: sp.Name, Seq: sp.Seq, CallKind: sp.CallKind, ThreadID: sp.ThreadID,
		}
		if len(sp.Output) > 0 {
			// A malformed optional output payload contributes no visible text.
			_ = json.Unmarshal(sp.Output, &span.Output)
		}
		trace.Spans = append(trace.Spans, span)
	}
	return trace, nil
}

func (c *APIClient) getJSON(ctx context.Context, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errors.New("not found")
	}
	if resp.StatusCode != http.StatusOK {
		body, readErr := readBounded(resp.Body, maxErrorResponseBytes)
		if readErr != nil {
			return fmt.Errorf("api returned status %d: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("api returned status %d: %s", resp.StatusCode, string(body))
	}
	body, err := readBounded(resp.Body, maxTraceResponseBytes)
	if err != nil {
		return fmt.Errorf("reading response: %w", err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

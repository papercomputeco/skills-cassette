package skill

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// transcriptConfig is the resolved option set for BuildSessionTranscript.
type transcriptConfig struct {
	timeFilter     *GenerateOptions
	traceID        string // when non-empty, render only this turn
	omitSpanDetail bool   // when true, drop the [tools] span-detail lines
	maxBytes       int    // zero means unbounded
}

// TranscriptOption configures BuildSessionTranscript.
type TranscriptOption func(*transcriptConfig)

// WithoutSpanDetail renders only the [user]/[assistant] conversation, dropping
// the [tools] span-detail lines — the lean transcript behind skill
// --spans=false.
func WithoutSpanDetail() TranscriptOption {
	return func(c *transcriptConfig) { c.omitSpanDetail = true }
}

// WithTimeFilter applies the --since/--until turn window. A nil opts is a
// no-op, mirroring skill generation's filtering.
func WithTimeFilter(opts *GenerateOptions) TranscriptOption {
	return func(c *transcriptConfig) { c.timeFilter = opts }
}

// WithTraceFilter restricts the transcript to a single turn (the --trace
// case). Other turns in the session are dropped before rendering.
func WithTraceFilter(traceID string) TranscriptOption {
	return func(c *transcriptConfig) { c.traceID = traceID }
}

// ErrTranscriptByteLimit is returned as soon as rendering the next transcript
// line would exceed the configured byte budget. The renderer never appends a
// partial line and does not fetch any later turn after the limit is reached.
var ErrTranscriptByteLimit = errors.New("session transcript exceeds its byte limit")

// WithTranscriptByteLimit bounds the rendered transcript. Enforcement happens
// while turns are rendered, before fetching the next trace, rather than after
// an oversized transcript has already been accumulated.
func WithTranscriptByteLimit(maxBytes int) TranscriptOption {
	return func(c *transcriptConfig) { c.maxBytes = maxBytes }
}

// BuildSessionTranscript renders the turn-grain transcript for one
// product session (a /v1/sessions UUID). It walks the trace surface:
// TraceSummaries for the session's user-visible turns, then Trace for
// each turn's spans. Per turn it emits the user prompt, then the
// main-thread conversation-spine ("main" call-kind, no sub-thread) llm
// responses in span order with tool usage summarized between them.
// Thinking blocks are dropped; when a turn carries no spine text the
// derive-time response preview stands in. Synthetic turns (compaction,
// resume replay) and turns outside the time window are filtered out.
//
// This is the single transcript code path shared by skill generation and
// generation.
func BuildSessionTranscript(ctx context.Context, query Querier, sessionID string, opts ...TranscriptOption) (string, error) {
	turns, err := SessionTurns(ctx, query, sessionID, opts...)
	if err != nil {
		return "", err
	}

	cfg := &transcriptConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.maxBytes < 0 {
		return "", errors.New("transcript byte limit must not be negative")
	}

	renderer := transcriptRenderer{maxBytes: cfg.maxBytes}
	for _, turn := range turns {
		if renderer.maxBytes > 0 && renderer.builder.Len() >= renderer.maxBytes {
			return "", fmt.Errorf("%w: configured limit %d bytes", ErrTranscriptByteLimit, renderer.maxBytes)
		}
		if err := writeTurn(ctx, &renderer, query, turn, !cfg.omitSpanDetail); err != nil {
			return "", err
		}
	}
	return renderer.String(), nil
}

// SessionTurns returns the filtered, user-visible turns of a session
// after applying any --trace and time-window options. It is the shared
// turn-resolution step behind BuildSessionTranscript and the structured
// (jsonl) export. Returns an error when no turns remain.
func SessionTurns(ctx context.Context, query Querier, sessionID string, opts ...TranscriptOption) ([]TraceSummary, error) {
	cfg := &transcriptConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	turns, err := query.TraceSummaries(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("load session %s: %w", sessionID, err)
	}

	turns = filterTurns(turns, cfg.timeFilter)

	if cfg.traceID != "" {
		var matched []TraceSummary
		for _, t := range turns {
			if t.TraceID == cfg.traceID {
				matched = append(matched, t)
			}
		}
		turns = matched
		if len(turns) == 0 {
			return nil, fmt.Errorf("trace %s not found in session %s", cfg.traceID, sessionID)
		}
		return turns, nil
	}

	if len(turns) == 0 {
		return nil, fmt.Errorf("%w: session %s", ErrNoTurns, sessionID)
	}
	return turns, nil
}

// TurnTranscript renders the [user]/[assistant]/[tools] lines for a
// single resolved turn, used when exporting one turn at a time.
func TurnTranscript(ctx context.Context, query Querier, turn TraceSummary) string {
	renderer := transcriptRenderer{}
	_ = writeTurn(ctx, &renderer, query, turn, true)
	return renderer.String()
}

type transcriptRenderer struct {
	builder  strings.Builder
	maxBytes int
}

func (r *transcriptRenderer) String() string { return r.builder.String() }

func (r *transcriptRenderer) writeParts(parts ...string) error {
	total := 0
	for _, part := range parts {
		if len(part) > int(^uint(0)>>1)-total {
			return fmt.Errorf("%w: configured limit %d bytes", ErrTranscriptByteLimit, r.maxBytes)
		}
		total += len(part)
	}
	if r.maxBytes > 0 && total > r.maxBytes-r.builder.Len() {
		return fmt.Errorf("%w: configured limit %d bytes", ErrTranscriptByteLimit, r.maxBytes)
	}
	for _, part := range parts {
		r.builder.WriteString(part)
	}
	return nil
}

// writeTurn renders one turn's prompt and spine responses. The user line is
// budgeted before the trace fetch, so a prompt that exhausts the budget stops
// iteration without fetching detail that can no longer be rendered.
func writeTurn(ctx context.Context, renderer *transcriptRenderer, query Querier, turn TraceSummary, includeSpanDetail bool) error {
	if turn.UserPrompt != "" {
		if err := renderer.writeParts("[user] ", turn.UserPrompt, "\n"); err != nil {
			return err
		}
	}
	trace, err := query.Trace(ctx, turn.TraceID)
	if err != nil || trace == nil {
		if turn.ResponsePreview != "" {
			return renderer.writeParts("[assistant] ", turn.ResponsePreview, "\n")
		}
		return nil
	}

	wrote, err := writeSpineResponses(renderer, trace.Spans, includeSpanDetail)
	if err != nil {
		return err
	}
	if !wrote && turn.ResponsePreview != "" {
		return renderer.writeParts("[assistant] ", turn.ResponsePreview, "\n")
	}
	return nil
}

// filterTurns drops synthetic turns (compaction seams, resume replays —
// no user intent to extract from) and applies the --since/--until
// window at turn grain.
func filterTurns(turns []TraceSummary, opts *GenerateOptions) []TraceSummary {
	var filtered []TraceSummary
	for _, turn := range turns {
		if turn.Synthetic != "" {
			continue
		}
		if opts != nil {
			if opts.Since != nil && turn.StartedAt.Before(*opts.Since) {
				continue
			}
			if opts.Until != nil && turn.StartedAt.After(*opts.Until) {
				continue
			}
		}
		filtered = append(filtered, turn)
	}
	return filtered
}

// writeSpineResponses walks one turn's spans in presentation order,
// emitting an [assistant] line per conversation-spine llm span with
// text and a [tools] summary line for the tool calls in between.
// Offshoot and injected call kinds, and subagent threads, are skipped.
// Reports whether any assistant text was written.
func writeSpineResponses(renderer *transcriptRenderer, spans []Span, includeSpanDetail bool) (bool, error) {
	wrote := false
	pendingTools := map[string]int{}
	var pendingOrder []string

	flushTools := func() error {
		if !includeSpanDetail || len(pendingOrder) == 0 {
			pendingTools = map[string]int{}
			pendingOrder = nil
			return nil
		}
		parts := []string{"[tools] "}
		for index, name := range pendingOrder {
			if index > 0 {
				parts = append(parts, ", ")
			}
			parts = append(parts, name)
			if count := pendingTools[name]; count > 1 {
				parts = append(parts, fmt.Sprintf(" ×%d", count))
			}
		}
		parts = append(parts, "\n")
		if err := renderer.writeParts(parts...); err != nil {
			return err
		}
		pendingTools = map[string]int{}
		pendingOrder = nil
		return nil
	}

	for _, sp := range spans {
		switch sp.Kind {
		case "tool":
			if sp.ThreadID != "" {
				continue
			}
			if _, seen := pendingTools[sp.Name]; !seen {
				pendingOrder = append(pendingOrder, sp.Name)
			}
			pendingTools[sp.Name]++
		case "llm":
			if sp.CallKind != "main" || sp.ThreadID != "" {
				continue
			}
			visibleBlocks := make([]string, 0, len(sp.Output))
			for _, block := range sp.Output {
				if block.Type == "text" && block.Text != "" {
					visibleBlocks = append(visibleBlocks, block.Text)
				}
			}
			if len(visibleBlocks) == 0 {
				continue
			}
			if err := flushTools(); err != nil {
				return wrote, err
			}
			parts := []string{"[assistant] "}
			for index, text := range visibleBlocks {
				if index > 0 {
					parts = append(parts, "\n")
				}
				parts = append(parts, text)
			}
			parts = append(parts, "\n")
			if err := renderer.writeParts(parts...); err != nil {
				return wrote, err
			}
			wrote = true
		}
	}
	if err := flushTools(); err != nil {
		return wrote, err
	}
	return wrote, nil
}

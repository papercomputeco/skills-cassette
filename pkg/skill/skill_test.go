package skill_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

type fakeQuerier struct {
	summaries []skill.TraceSummary
	sessions  map[string][]skill.TraceSummary
	traces    map[string]*skill.Trace
}

type countingQuerier struct {
	summaries []skill.TraceSummary
	traces    map[string]*skill.Trace
	traceIDs  []string
}

func (q *countingQuerier) TraceSummaries(context.Context, string) ([]skill.TraceSummary, error) {
	return q.summaries, nil
}

func (q *countingQuerier) Trace(_ context.Context, traceID string) (*skill.Trace, error) {
	q.traceIDs = append(q.traceIDs, traceID)
	return q.traces[traceID], nil
}

func (f fakeQuerier) TraceSummaries(_ context.Context, sessionID string) ([]skill.TraceSummary, error) {
	if f.sessions != nil {
		return f.sessions[sessionID], nil
	}
	return f.summaries, nil
}

func (f fakeQuerier) Trace(_ context.Context, id string) (*skill.Trace, error) {
	trace, ok := f.traces[id]
	if !ok {
		return nil, errors.New("missing trace")
	}
	return trace, nil
}

var _ = Describe("skill generation boundary", func() {
	It("builds only the main thread, filters synthetic turns, and falls back to previews", func() {
		q := fakeQuerier{
			summaries: []skill.TraceSummary{
				{TraceID: "one", UserPrompt: "real prompt", StartedAt: time.Now()},
				{TraceID: "preview", UserPrompt: "preview prompt", ResponsePreview: "preview answer", StartedAt: time.Now()},
				{TraceID: "synthetic", UserPrompt: "hidden", Synthetic: "compaction", StartedAt: time.Now()},
			},
			traces: map[string]*skill.Trace{
				"one": {TraceID: "one", Spans: []skill.Span{
					{Kind: "llm", CallKind: "offshoot:title", Output: []skill.ContentBlock{{Type: "text", Text: "offshoot"}}},
					{Kind: "llm", CallKind: "main", ThreadID: "subagent", Output: []skill.ContentBlock{{Type: "text", Text: "thread"}}},
					{Kind: "tool", Name: "Read"},
					{Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "thinking", Text: "private reasoning"}, {Type: "text", Text: "main answer"}}},
				}},
				"preview": {TraceID: "preview", Spans: []skill.Span{{Kind: "tool", Name: "Bash"}}},
			},
		}
		transcript, err := skill.BuildSessionTranscript(context.Background(), q, "session")
		Expect(err).NotTo(HaveOccurred())
		Expect(transcript).To(ContainSubstring("[user] real prompt"))
		Expect(transcript).To(ContainSubstring("[tools] Read"))
		Expect(transcript).To(ContainSubstring("[assistant] main answer"))
		Expect(transcript).To(ContainSubstring("[assistant] preview answer"))
		Expect(transcript).NotTo(ContainSubstring("offshoot"))
		Expect(transcript).NotTo(ContainSubstring("thread"))
		Expect(transcript).NotTo(ContainSubstring("hidden"))
		Expect(transcript).NotTo(ContainSubstring("private reasoning"))
	})

	It("enforces transcript budgets before fetching later traces", func() {
		query := &countingQuerier{
			summaries: []skill.TraceSummary{
				{TraceID: "first", UserPrompt: "first", StartedAt: time.Now()},
				{TraceID: "second", UserPrompt: strings.Repeat("x", 100), StartedAt: time.Now().Add(time.Second)},
			},
			traces: map[string]*skill.Trace{
				"first": {TraceID: "first", Spans: []skill.Span{{
					Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: "answer"}},
				}}},
				"second": {TraceID: "second"},
			},
		}
		firstTurnBytes := len("[user] first\n[assistant] answer\n")
		transcript, err := skill.BuildSessionTranscript(context.Background(), query, "session",
			skill.WithTranscriptByteLimit(firstTurnBytes+20))
		Expect(transcript).To(BeEmpty())
		Expect(errors.Is(err, skill.ErrTranscriptByteLimit)).To(BeTrue())
		Expect(query.traceIDs).To(Equal([]string{"first"}),
			"the over-budget second user line must stop before its trace fetch")

		oversized := &countingQuerier{
			summaries: []skill.TraceSummary{{
				TraceID: "never-fetched", UserPrompt: strings.Repeat("x", (1<<20)+1), StartedAt: time.Now(),
			}},
			traces: map[string]*skill.Trace{"never-fetched": {TraceID: "never-fetched"}},
		}
		llmCalls := 0
		generator := skill.NewGenerator(oversized, func(context.Context, string) (string, error) {
			llmCalls++
			return "", nil
		})
		_, err = generator.GenerateSessionCandidate(context.Background(), "session", "", "bounded", "workflow", nil)
		Expect(errors.Is(err, skill.ErrTranscriptByteLimit)).To(BeTrue())
		Expect(oversized.traceIDs).To(BeEmpty())
		Expect(llmCalls).To(BeZero())
	})

	It("retries malformed JSON and renders SKILL.md", func() {
		q := fakeQuerier{summaries: []skill.TraceSummary{{TraceID: "one", UserPrompt: "do it", StartedAt: time.Now()}}, traces: map[string]*skill.Trace{"one": {TraceID: "one", Spans: []skill.Span{{Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: "done"}}}}}}}
		calls := 0
		gen := skill.NewGenerator(q, func(_ context.Context, prompt string) (string, error) {
			calls++
			if calls == 1 {
				return "not-json", nil
			}
			Expect(prompt).To(ContainSubstring("Return ONLY valid JSON"))
			return `{"skill":{"name":"ignored","description":"Use when doing it","tags":["workflow"],"content":"## Steps\\n\\n1. Do it"},"insights":[{"kind":"pattern","summary":"Repeat the workflow","evidence":"The session completed it."}]}`, nil
		})
		candidate, err := gen.GenerateSessionCandidate(context.Background(), "session", "", "do-it", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(calls).To(Equal(2))
		Expect(candidate.Insights).To(HaveLen(1))
		markdown := skill.RenderSkillMD(&candidate.Skill)
		Expect(markdown).To(ContainSubstring(`name: "do-it"`))
		Expect(markdown).To(ContainSubstring(`sessions: ["session"]`))
		Expect(markdown).To(ContainSubstring("## Steps"))
		Expect(markdown).To(HaveSuffix("\n"))
	})

	It("strictly decodes insights and bounds fields by Unicode code points", func() {
		maxRunes := strings.Repeat("界", 2048)
		validResponse, err := json.Marshal(map[string]any{
			"skill": map[string]any{
				"name": "bounded", "description": "Use when checking insight limits.",
				"tags": []string{"workflow"}, "content": "# Bounded",
			},
			"insights": []map[string]string{{"kind": maxRunes, "summary": maxRunes, "evidence": maxRunes}},
		})
		Expect(err).NotTo(HaveOccurred())
		gen := skill.NewGenerator(fakeQuerier{}, func(context.Context, string) (string, error) {
			return string(validResponse), nil
		})
		candidate, err := gen.GenerateContextCandidate(context.Background(), "bounded context", "bounded", "workflow")
		Expect(err).NotTo(HaveOccurred(), "2048 multibyte runes are legal even though they exceed 2048 bytes")
		Expect(candidate.Insights).To(HaveLen(1))
		Expect(candidate.Insights[0].Summary).To(HaveLen(len(maxRunes)))

		for _, insightJSON := range []string{
			`{"kind":"fact","summary":"bounded","evidence":"source","unsafe":"legacy"}`,
			`{"kind":"fact","kind":"duplicate","summary":"bounded","evidence":"source"}`,
			`{"kind":"fact","summary":"bounded"}`,
			`{"kind":"fact","summary":"` + strings.Repeat("界", 2049) + `","evidence":"source"}`,
		} {
			calls := 0
			bad := skill.NewGenerator(fakeQuerier{}, func(context.Context, string) (string, error) {
				calls++
				return `{"skill":{"name":"bad","description":"Use when testing.","tags":[],"content":"# Bad"},"insights":[` + insightJSON + `]}`, nil
			})
			_, err = bad.GenerateContextCandidate(context.Background(), "bounded context", "bad", "workflow")
			Expect(err).To(HaveOccurred())
			Expect(calls).To(Equal(3))
		}
	})

	It("validates every generated revision snapshot field before success", func() {
		invalidSkills := []map[string]any{
			{"name": strings.Repeat("n", 1025), "description": "Use when testing.", "tags": []string{}, "content": "# Bad"},
			{"name": "bad", "description": "Use when testing.\x00", "tags": []string{}, "content": "# Bad"},
			{"name": "bad", "description": "Use when testing.", "tags": make([]string, 65), "content": "# Bad"},
			{"name": "bad", "description": "Use when testing.", "tags": []string{strings.Repeat("t", 257)}, "content": "# Bad"},
			{"name": "bad", "description": "Use when testing.", "tags": []string{"same", " same "}, "content": "# Bad"},
			{"name": "bad", "description": "Use when testing.", "tags": []string{}, "content": "# Bad\x00"},
		}
		for index, invalidSkill := range invalidSkills {
			if tags, ok := invalidSkill["tags"].([]string); ok {
				for tagIndex := range tags {
					if tags[tagIndex] == "" {
						tags[tagIndex] = fmt.Sprintf("tag-%d", tagIndex)
					}
				}
			}
			response, err := json.Marshal(map[string]any{"skill": invalidSkill, "insights": []any{}})
			Expect(err).NotTo(HaveOccurred())
			calls := 0
			gen := skill.NewGenerator(fakeQuerier{}, func(context.Context, string) (string, error) {
				calls++
				return string(response), nil
			})
			_, err = gen.GenerateContextCandidate(context.Background(), "bounded context", "", "workflow")
			Expect(err).To(HaveOccurred(), "invalid snapshot %d", index)
			Expect(calls).To(Equal(3), "snapshot validation participates in bounded malformed-model retries")
		}
	})

	It("renders every immutable input snapshot field for each isolated candidate", func() {
		response := `{"skill":{"name":"generated","description":"Use when testing snapshot intent.","tags":[],"content":"# Generated"},"insights":[]}`
		var prompts []string
		gen := skill.NewGenerator(fakeQuerier{}, func(_ context.Context, prompt string) (string, error) {
			prompts = append(prompts, prompt)
			return response, nil
		})
		base := skill.CandidateInputSnapshot{
			Name: "snapshot-name", Description: "snapshot-description", Type: "workflow",
			Tags: []string{"snapshot-tag"}, Content: "# Snapshot content", IsAIGenerated: false,
			SourceSessionIDs: []string{"snapshot-source"},
		}
		generate := func(snapshot skill.CandidateInputSnapshot, transcript, source string) string {
			before := len(prompts)
			candidate, err := gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
				InputSnapshot: snapshot, AuthorContext: "author intent",
				Transcript: transcript, SourceSessionID: source,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(candidate).NotTo(BeNil())
			Expect(prompts).To(HaveLen(before + 1))
			return prompts[before]
		}
		basePrompt := generate(base, "ONLY_BASE_TRANSCRIPT", "candidate-source")
		const opening = "<immutable-input-snapshot>\n"
		const closing = "\n</immutable-input-snapshot>"
		start := strings.Index(basePrompt, opening)
		end := strings.Index(basePrompt, closing)
		Expect(start).To(BeNumerically(">=", 0))
		Expect(end).To(BeNumerically(">", start))
		var rendered skill.CandidateInputSnapshot
		Expect(json.Unmarshal([]byte(basePrompt[start+len(opening):end]), &rendered)).To(Succeed())
		Expect(rendered).To(Equal(base))

		mutations := []func(*skill.CandidateInputSnapshot){
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.Name = "changed-name" },
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.Description = "changed-description" },
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.Type = "domain-knowledge" },
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.Tags = []string{"changed-tag"} },
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.Content = "# Changed content" },
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.IsAIGenerated = true },
			func(snapshot *skill.CandidateInputSnapshot) { snapshot.SourceSessionIDs = []string{"changed-source"} },
		}
		for index, mutate := range mutations {
			changed := base
			changed.Tags = append([]string(nil), base.Tags...)
			changed.SourceSessionIDs = append([]string(nil), base.SourceSessionIDs...)
			mutate(&changed)
			prompt := generate(changed, "ONLY_BASE_TRANSCRIPT", "candidate-source")
			Expect(prompt).NotTo(Equal(basePrompt), "snapshot field %d must affect the prompt", index)
			Expect(strings.Count(prompt, "ONLY_BASE_TRANSCRIPT")).To(Equal(1))
		}
		otherPrompt := generate(base, "ONLY_OTHER_TRANSCRIPT", "other-candidate-source")
		Expect(otherPrompt).To(ContainSubstring("ONLY_OTHER_TRANSCRIPT"))
		Expect(otherPrompt).NotTo(ContainSubstring("ONLY_BASE_TRANSCRIPT"),
			"independent prompts must never acquire another candidate's transcript")
		Expect(basePrompt).NotTo(ContainSubstring("ONLY_OTHER_TRANSCRIPT"))
	})

	It("admits one maximum legal immutable snapshot with one maximum transcript", func() {
		tags := make([]string, 64)
		for index := range tags {
			tags[index] = fmt.Sprintf("t%03d", index) + strings.Repeat("<", 252)
		}
		sources := make([]string, 100)
		for index := range sources {
			sources[index] = fmt.Sprintf("s%03d", index) + strings.Repeat("<", 252)
		}
		snapshot := skill.CandidateInputSnapshot{
			Name: strings.Repeat("<", 1024), Description: strings.Repeat("<", 1<<20),
			Type: "workflow", Tags: tags, Content: strings.Repeat("<", 1<<20),
			IsAIGenerated: true, SourceSessionIDs: sources,
		}
		calls := 0
		gen := skill.NewGenerator(fakeQuerier{}, func(_ context.Context, prompt string) (string, error) {
			calls++
			Expect(prompt).To(ContainSubstring("<immutable-input-snapshot>"))
			Expect(prompt).To(ContainSubstring("<session-transcript>"))
			return `{"skill":{"name":"generated","description":"Use when testing maximum input.","tags":[],"content":"# Generated"},"insights":[]}`, nil
		})
		candidate, err := gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			InputSnapshot: snapshot, Transcript: strings.Repeat("t", 1<<20),
			SourceSessionID: "candidate-source",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(candidate).NotTo(BeNil())
		Expect(calls).To(Equal(1))
	})

	It("retains immutable snapshot intent in bounded synthesis without transcripts", func() {
		snapshot := skill.CandidateInputSnapshot{
			Name: "author-intent", Description: "Keep this intent", Type: "workflow",
			Tags: []string{"intent"}, Content: "# Original intent", IsAIGenerated: true,
			SourceSessionIDs: []string{"prior-source"},
		}
		var prompt string
		gen := skill.NewGenerator(fakeQuerier{}, func(_ context.Context, value string) (string, error) {
			prompt = value
			return `{"skill":{"name":"refined","description":"Use when refining.","tags":[],"content":"# Refined"},"insights":[]}`, nil
		})
		candidate, err := gen.SynthesizeCandidate(context.Background(), skill.SynthesisRequest{
			InputSnapshot: snapshot, AuthorContext: "keep author intent",
			Winner: skill.Candidate{Skill: skill.Skill{
				Name: "winner", Description: "Use when winning.", Type: "workflow", Content: "# Winner",
			}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(candidate).NotTo(BeNil())
		Expect(prompt).To(ContainSubstring(`"inputSnapshot"`))
		Expect(prompt).To(ContainSubstring("Original intent"))
		Expect(prompt).To(ContainSubstring("keep author intent"))
		Expect(prompt).NotTo(ContainSubstring("session-transcript"))
	})

	It("accepts one MiB transcripts and bounds author context by Unicode code points", func() {
		response := `{"skill":{"name":"bounded","description":"Use when testing bounded prompts.","tags":["workflow"],"content":"## Steps\\n\\n1. Stay bounded."},"insights":[]}`
		calls := 0
		gen := skill.NewGenerator(fakeQuerier{}, func(_ context.Context, prompt string) (string, error) {
			calls++
			if calls == 1 {
				Expect(len(prompt)).To(BeNumerically(">", 1<<20))
			}
			return response, nil
		})
		candidate, err := gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			Name: "bounded", SkillType: "workflow", SourceSessionID: "session",
			Transcript: strings.Repeat("t", 1<<20),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(candidate).NotTo(BeNil())
		Expect(calls).To(Equal(1))

		_, err = gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			Name: "too-large", SkillType: "workflow", SourceSessionID: "session",
			Transcript: strings.Repeat("t", (1<<20)+1),
		})
		Expect(err).To(MatchError(ContainSubstring("1048576 byte limit")))
		Expect(calls).To(Equal(1))

		unicodeContext := strings.Repeat("界", 32<<10)
		candidate, err = gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			Name: "unicode", SkillType: "workflow", AuthorContext: unicodeContext,
		})
		Expect(err).NotTo(HaveOccurred(), "multibyte context is bounded by code points, not bytes")
		Expect(candidate).NotTo(BeNil())
		_, err = gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			Name: "unicode", SkillType: "workflow", AuthorContext: unicodeContext + "界",
		})
		Expect(err).To(MatchError(ContainSubstring("Unicode code point limit")))
		Expect(calls).To(Equal(2))
	})

	It("does not retry oversized successful candidate responses", func() {
		calls := 0
		gen := skill.NewGenerator(fakeQuerier{}, func(context.Context, string) (string, error) {
			calls++
			return strings.Repeat("x", (1<<20)+1), nil
		})

		_, err := gen.GenerateContextCandidate(context.Background(), "write a bounded skill", "bounded", "workflow")
		Expect(err).To(MatchError(ContainSubstring("candidate response exceeds")))
		var callError *skill.ExternalCallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeFalse())
		Expect(calls).To(Equal(1), "oversized successful responses must not enter the malformed-JSON retry loop")
	})

	It("generator_never_combines_session_transcripts", func() {
		now := time.Now()
		q := fakeQuerier{
			sessions: map[string][]skill.TraceSummary{
				"session-a": {{TraceID: "trace-a", UserPrompt: "UNIQUE_ALPHA", StartedAt: now}},
				"session-b": {{TraceID: "trace-b", UserPrompt: "UNIQUE_BRAVO", StartedAt: now}},
			},
			traces: map[string]*skill.Trace{
				"trace-a": {TraceID: "trace-a", Spans: []skill.Span{{Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: "alpha answer"}}}}},
				"trace-b": {TraceID: "trace-b", Spans: []skill.Span{{Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: "bravo answer"}}}}},
			},
		}
		var prompts []string
		gen := skill.NewGenerator(q, func(_ context.Context, prompt string) (string, error) {
			prompts = append(prompts, prompt)
			return `{"skill":{"name":"candidate","description":"Use when repeating this workflow","tags":["workflow"],"content":"## Steps\\n\\n1. Repeat it."},"insights":[{"kind":"evidence","summary":"A reusable step","evidence":"The source completed the step."}]}`, nil
		})

		first, err := gen.GenerateSessionCandidate(context.Background(), "session-a", "prefer safe steps", "", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		second, err := gen.GenerateSessionCandidate(context.Background(), "session-b", "prefer safe steps", "", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		contextOnly, err := gen.GenerateContextCandidate(context.Background(), "write a release checklist", "release-checklist", "workflow")
		Expect(err).NotTo(HaveOccurred())

		Expect(prompts).To(HaveLen(3))
		Expect(prompts[0]).To(ContainSubstring("UNIQUE_ALPHA"))
		Expect(strings.Count(prompts[0], "UNIQUE_ALPHA")).To(Equal(1))
		Expect(prompts[0]).NotTo(ContainSubstring("UNIQUE_BRAVO"))
		Expect(prompts[1]).To(ContainSubstring("UNIQUE_BRAVO"))
		Expect(strings.Count(prompts[1], "UNIQUE_BRAVO")).To(Equal(1))
		Expect(prompts[1]).NotTo(ContainSubstring("UNIQUE_ALPHA"))
		Expect(prompts[2]).To(ContainSubstring("write a release checklist"))
		Expect(prompts[2]).NotTo(ContainSubstring("UNIQUE_ALPHA"))
		Expect(prompts[2]).NotTo(ContainSubstring("UNIQUE_BRAVO"))
		Expect(first.Skill.Sessions).To(Equal([]string{"session-a"}))
		Expect(second.Skill.Sessions).To(Equal([]string{"session-b"}))
		Expect(contextOnly.Skill.Sessions).To(BeEmpty())
		Expect(first.Skill.Name).NotTo(BeEmpty())
		Expect(first.Skill.Description).NotTo(BeEmpty())
		Expect(first.Skill.Content).NotTo(BeEmpty())
		Expect(first.Insights).To(HaveLen(1))

		calls := len(prompts)
		_, err = gen.Generate(context.Background(), []string{"session-a", "session-b"}, "combined", "workflow", nil)
		Expect(err).To(MatchError(ContainSubstring("one session")))
		_, err = gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			Name: "missing-source", SkillType: "workflow", Transcript: "UNSCOPED_TRANSCRIPT",
		})
		Expect(err).To(MatchError(ContainSubstring("transcript and its single source session ID")))
		_, err = gen.GenerateCandidate(context.Background(), skill.CandidateRequest{
			Name: "missing-transcript", SkillType: "workflow", AuthorContext: "context",
			SourceSessionID: "session-a",
		})
		Expect(err).To(MatchError(ContainSubstring("transcript and its single source session ID")))
		Expect(prompts).To(HaveLen(calls), "invalid or multi-session inputs must not reach inference")
	})

	It("quotes generated frontmatter as YAML-safe JSON values", func() {
		markdown := skill.RenderSkillMD(&skill.Skill{
			Name:        "unsafe\n---\nname: injected",
			Description: "Use when: values contain # or [punctuation]",
			Version:     "0.1.0",
			Tags:        []string{"comma,tag", "bracket]tag"},
			Content:     "## Safe body",
			Sessions:    []string{"session:one"},
		})

		Expect(strings.Count(markdown, "---\n")).To(Equal(2))
		Expect(markdown).To(ContainSubstring(`name: "unsafe\n---\nname: injected"`))
		Expect(markdown).To(ContainSubstring(`tags: ["comma,tag","bracket]tag"]`))
		Expect(markdown).To(ContainSubstring(`sessions: ["session:one"]`))
	})

	It("rejects invalid input", func() {
		gen := skill.NewGenerator(fakeQuerier{}, func(context.Context, string) (string, error) { return "", nil })
		_, err := gen.Generate(context.Background(), nil, "x", "workflow", nil)
		Expect(err).To(MatchError(ContainSubstring("at least one session")))
		_, err = gen.Generate(context.Background(), []string{"x"}, "x", "unknown", nil)
		Expect(err).To(MatchError(ContainSubstring("invalid skill type")))
	})
})

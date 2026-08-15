package skill_test

import (
	"context"
	"errors"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

type fakeQuerier struct {
	summaries []skill.TraceSummary
	traces    map[string]*skill.Trace
}

func (f fakeQuerier) TraceSummaries(context.Context, string) ([]skill.TraceSummary, error) {
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

	It("retries malformed JSON and renders SKILL.md", func() {
		q := fakeQuerier{summaries: []skill.TraceSummary{{TraceID: "one", UserPrompt: "do it", StartedAt: time.Now()}}, traces: map[string]*skill.Trace{"one": {TraceID: "one", Spans: []skill.Span{{Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: "done"}}}}}}}
		calls := 0
		gen := skill.NewGenerator(q, func(_ context.Context, prompt string) (string, error) {
			calls++
			if calls == 1 {
				return "not-json", nil
			}
			Expect(prompt).To(ContainSubstring("Return ONLY valid JSON"))
			Expect(prompt).To(ContainSubstring("Author brief:\nMake this reusable"))
			Expect(prompt).To(ContainSubstring("1. Add verification"))
			return `{"name":"do-it","description":"Use when doing it","tags":["workflow"],"content":"## Steps\\n\\n1. Do it"}`, nil
		})
		generated, err := gen.Generate(context.Background(), []string{"session"}, "Make this reusable", []string{"Add verification"}, "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(calls).To(Equal(2))
		markdown := skill.RenderSkillMD(generated)
		Expect(markdown).To(ContainSubstring(`name: "do-it"`))
		Expect(markdown).To(ContainSubstring(`sessions: ["session"]`))
		Expect(markdown).To(ContainSubstring("## Steps"))
		Expect(markdown).To(HaveSuffix("\n"))
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

	It("generates from a brief without sessions", func() {
		gen := skill.NewGenerator(nil, func(_ context.Context, prompt string) (string, error) {
			Expect(prompt).To(ContainSubstring("Author brief:\nDocument the release process"))
			return `{"name":"Release process","description":"Use when releasing.","content":"## Steps\\n1. Release."}`, nil
		})
		generated, err := gen.Generate(context.Background(), nil, "Document the release process", nil, "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(generated.Sessions).To(BeEmpty())
	})

	It("revises content without persistence", func() {
		gen := skill.NewGenerator(nil, func(_ context.Context, prompt string) (string, error) {
			Expect(prompt).To(ContainSubstring("Add verification"))
			return `{"content":"## Steps\\n1. Do it.\\n\\n## Verification\\nCheck it."}`, nil
		})
		content, err := gen.Revise(context.Background(), "## Steps\n1. Do it.", "Add verification")
		Expect(err).NotTo(HaveOccurred())
		Expect(content).To(ContainSubstring("## Verification"))
	})

	It("rejects invalid input", func() {
		gen := skill.NewGenerator(fakeQuerier{}, func(context.Context, string) (string, error) { return "", nil })
		_, err := gen.Generate(context.Background(), nil, "", nil, "workflow", nil)
		Expect(err).To(MatchError(ContainSubstring("session ID or a brief")))
		_, err = gen.Generate(context.Background(), []string{"x"}, "", nil, "unknown", nil)
		Expect(err).To(MatchError(ContainSubstring("invalid skill type")))
	})
})

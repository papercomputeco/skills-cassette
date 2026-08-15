package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const maxGenerateRetries = 3

// LLMCallFunc is the configured inference boundary used for skill extraction.
type LLMCallFunc func(ctx context.Context, prompt string) (string, error)

// GenerateOptions controls filtering for skill generation.
type GenerateOptions struct {
	Since *time.Time // only include turns starting on or after this time
	Until *time.Time // only include turns starting on or before this time
}

// Generator extracts and revises skills via an LLM.
//
// Transcripts are built from the span model: each user-visible turn
// contributes its prompt plus the conversation-spine ("main" call-kind,
// main-thread) llm span outputs, with tool usage summarized between
// responses. Offshoot calls (permission checks, title-gen, …) and
// injected context never reach the prompt — the extraction LLM sees the
// actual conversation, not the harness's shadow traffic.
type Generator struct {
	query   Querier
	llmCall LLMCallFunc
}

// NewGenerator creates a new skill Generator.
func NewGenerator(query Querier, llmCall LLMCallFunc) *Generator {
	return &Generator{query: query, llmCall: llmCall}
}

// Generate returns an unpersisted skill draft from source sessions, an author
// brief, or both. Feedback is replayed oldest-first on every regeneration.
func (g *Generator) Generate(ctx context.Context, sessionIDs []string, brief string, feedback []string, skillType string, opts *GenerateOptions) (*Skill, error) {
	if len(sessionIDs) == 0 && strings.TrimSpace(brief) == "" {
		return nil, errors.New("at least one session ID or a brief is required")
	}
	if !ValidSkillType(skillType) {
		return nil, fmt.Errorf("invalid skill type %q; valid types: %s", skillType, strings.Join(SkillTypes, ", "))
	}
	if len(sessionIDs) > 0 && g.query == nil {
		return nil, errors.New("session generation requires a transcript querier")
	}

	transcripts := make([]string, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		transcript, err := BuildSessionTranscript(ctx, g.query, sessionID, WithTimeFilter(opts))
		if err != nil {
			return nil, err
		}
		transcripts = append(transcripts, transcript)
	}

	// Truncate large transcripts at a session boundary. Keep an oversized first
	// session rather than silently generating from nothing.
	const maxChars = 30000
	var totalLen int
	for i, transcript := range transcripts {
		totalLen += len(transcript)
		if i > 0 {
			totalLen += len("\n---\n")
		}
		if i > 0 && totalLen > maxChars {
			transcripts = transcripts[:i]
			fmt.Fprintf(os.Stderr, "warning: transcript truncated to %d of %d session(s) to fit within %d char limit\n",
				len(transcripts), len(sessionIDs), maxChars)
			break
		}
	}

	basePrompt := buildSkillPrompt(strings.Join(transcripts, "\n---\n"), brief, feedback, skillType)
	var lastErr error
	for attempt := range maxGenerateRetries {
		prompt := basePrompt
		if attempt > 0 {
			prompt += "\n\nReturn ONLY valid JSON, no markdown."
		}
		response, err := g.llmCall(ctx, prompt)
		if err != nil {
			return nil, fmt.Errorf("llm call: %w", err)
		}
		draft, err := parseSkillResponse(response)
		if err != nil {
			lastErr = fmt.Errorf("parse response (attempt %d): %w", attempt+1, err)
			continue
		}
		draft.Name = strings.TrimSpace(draft.Name)
		draft.Type = skillType
		draft.Sessions = sessionIDs
		draft.Version = "0.1.0"
		draft.CreatedAt = time.Now()
		return draft, nil
	}
	return nil, lastErr
}

// Revise rewrites an unpersisted working document. Saving remains the caller's
// explicit next action.
func (g *Generator) Revise(ctx context.Context, content, instruction string) (string, error) {
	if strings.TrimSpace(content) == "" || strings.TrimSpace(instruction) == "" {
		return "", errors.New("content and instruction are required")
	}
	basePrompt := fmt.Sprintf(`Rewrite the complete skill document according to the author's instruction.
Preserve useful material that the instruction does not change.
Return ONLY valid JSON in this shape: {"content":"the complete rewritten markdown"}.

Instruction:
%s

Current document:
%s`, instruction, content)

	var lastErr error
	for attempt := range maxGenerateRetries {
		prompt := basePrompt
		if attempt > 0 {
			prompt += "\n\nReturn ONLY valid JSON, no markdown."
		}
		response, err := g.llmCall(ctx, prompt)
		if err != nil {
			return "", fmt.Errorf("llm call: %w", err)
		}
		var revised struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(extractJSONObject(response), &revised); err != nil || strings.TrimSpace(revised.Content) == "" {
			lastErr = fmt.Errorf("parse response (attempt %d): invalid revision JSON", attempt+1)
			continue
		}
		return revised.Content, nil
	}
	return "", lastErr
}

func buildSkillPrompt(transcript, brief string, feedback []string, skillType string) string {
	feedbackText := "(none)"
	if len(feedback) > 0 {
		lines := make([]string, len(feedback))
		for i, note := range feedback {
			lines[i] = fmt.Sprintf("%d. %s", i+1, note)
		}
		feedbackText = strings.Join(lines, "\n")
	}
	if transcript == "" {
		transcript = "(none supplied)"
	}
	return fmt.Sprintf(`Create a reusable %s skill from the author's brief and any source transcripts.
The newest draft must satisfy every feedback note; notes are ordered oldest-first.

Return ONLY valid JSON with these fields:
{
  "name": "a concise human-readable title",
  "description": "A trigger description beginning with an action verb",
  "tags": ["relevant", "tags"],
  "content": "The complete markdown body with imperative steps"
}

Guidelines:
- Generalize the reusable technique rather than copying session-specific details
- Use ## headers and numbered steps
- Preserve important caveats and verification steps
- Use transcript [tools] lines when the workflow depends on specific tools

Author brief:
%s

Feedback:
%s

Transcript(s):
%s`, skillType, strings.TrimSpace(brief), feedbackText, transcript)
}

func parseSkillResponse(response string) (*Skill, error) {
	var draft Skill
	if err := json.Unmarshal(extractJSONObject(response), &draft); err != nil {
		return nil, fmt.Errorf("unmarshal skill JSON: %w", err)
	}
	if strings.TrimSpace(draft.Name) == "" || strings.TrimSpace(draft.Content) == "" {
		return nil, errors.New("generated skill requires name and content")
	}
	return &draft, nil
}

func extractJSONObject(response string) []byte {
	if start := strings.Index(response, "{"); start >= 0 {
		if end := strings.LastIndex(response, "}"); end > start {
			return []byte(response[start : end+1])
		}
	}
	return []byte(response)
}

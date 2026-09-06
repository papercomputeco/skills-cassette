// Package skill provides LLM-powered extraction of reusable patterns from
// tapes sessions, outputting Claude Code SKILL.md files.
package skill

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
)

// Skill represents a Claude Code skill extracted from session data.
type Skill struct {
	Name        string    `json:"name"`        // kebab-case identifier
	Description string    `json:"description"` // trigger description for Claude
	Version     string    `json:"version"`     // semver, default "0.1.0"
	Tags        []string  `json:"tags"`        // e.g. ["debugging", "react"]
	Type        string    `json:"type"`        // "workflow", "domain-knowledge", "prompt-template"
	Content     string    `json:"content"`     // markdown body (instructions)
	Sessions    []string  `json:"sessions"`    // source session IDs
	CreatedAt   time.Time `json:"created_at"`
}

// CandidateInputSnapshot is the complete immutable author intent persisted for
// one generation. It deliberately excludes stable skill identity and revision
// ancestry, which are not model inputs.
type CandidateInputSnapshot struct {
	Name             string   `json:"name"`
	Description      string   `json:"description"`
	Type             string   `json:"type"`
	Tags             []string `json:"tags"`
	Content          string   `json:"content"`
	IsAIGenerated    bool     `json:"isAiGenerated"`
	SourceSessionIDs []string `json:"sourceSessionIds"`
}

// CandidateRequest contains exactly one inference unit: either one raw session
// transcript or durable author context without a transcript. InputSnapshot is
// always rendered as a complete structured object. Name and SkillType remain as
// compatibility fields for callers that predate persisted generation snapshots.
type CandidateRequest struct {
	InputSnapshot   CandidateInputSnapshot
	Name            string
	SkillType       string
	AuthorContext   string
	Transcript      string
	SourceSessionID string
}

// CandidateInsight is bounded, structured evidence retained independently of
// the raw prompt so later synthesis never needs combined transcripts.
type CandidateInsight struct {
	Kind     string `json:"kind"`
	Summary  string `json:"summary"`
	Evidence string `json:"evidence"`
}

// UnmarshalJSON keeps model-produced insights on the same closed schema used
// by durable storage. Unknown and missing fields are rejected before they can
// be silently erased by ordinary struct decoding.
func (i *CandidateInsight) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errors.New("candidate insight must be an object")
	}
	seen := map[string]struct{}{}
	var kind, summary, evidence *string
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return tokenErr
		}
		name, ok := token.(string)
		if !ok {
			return errors.New("candidate insight field name is invalid")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("candidate insight field %q is duplicated", name)
		}
		seen[name] = struct{}{}
		var value string
		if err = decoder.Decode(&value); err != nil {
			return err
		}
		switch name {
		case "kind":
			kind = &value
		case "summary":
			summary = &value
		case "evidence":
			evidence = &value
		default:
			return fmt.Errorf("candidate insight field %q is unknown", name)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return errors.New("candidate insight object is incomplete")
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("candidate insight contains multiple JSON values")
		}
		return err
	}
	if kind == nil || summary == nil || evidence == nil {
		return errors.New("candidate insight requires kind, summary, and evidence")
	}
	i.Kind, i.Summary, i.Evidence = *kind, *summary, *evidence
	return nil
}

// Candidate is one complete generated skill and its bounded supporting
// insights.
type Candidate struct {
	Skill    Skill              `json:"skill"`
	Insights []CandidateInsight `json:"insights"`
}

// SynthesisFeedback is bounded structured quality context from one evaluated
// candidate. It intentionally contains no raw transcript.
type SynthesisFeedback struct {
	Insights  []CandidateInsight
	Findings  json.RawMessage
	Strengths json.RawMessage
}

// SynthesisRequest asks for at most one complete refinement of a winner using
// durable context and structured feedback only.
type SynthesisRequest struct {
	InputSnapshot CandidateInputSnapshot
	Name          string
	SkillType     string
	AuthorContext string
	Winner        Candidate
	Feedback      []SynthesisFeedback
}

// SkillTypes enumerates valid skill type values.
var SkillTypes = []string{"workflow", "domain-knowledge", "prompt-template"}

// ValidSkillType returns true if the given type is a recognized skill type.
func ValidSkillType(t string) bool {
	return slices.Contains(SkillTypes, t)
}

// ContentBlock is the minimal tapes-core content block shape needed by transcripts.
type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

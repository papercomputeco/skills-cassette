package generation

import (
	"context"

	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

// TranscriptLoader resolves one bounded transcript at a time.
type TranscriptLoader interface {
	LoadTranscript(context.Context, string) (string, error)
}

// TranscriptLoaderFunc adapts a function to TranscriptLoader.
type TranscriptLoaderFunc func(context.Context, string) (string, error)

func (f TranscriptLoaderFunc) LoadTranscript(ctx context.Context, sessionID string) (string, error) {
	return f(ctx, sessionID)
}

// CandidateGenerator is the inference-only boundary. Transcript loading stays
// separate so source-specific failures can be retained independently.
type CandidateGenerator interface {
	GenerateCandidate(context.Context, skill.CandidateRequest) (*skill.Candidate, error)
	SynthesizeCandidate(context.Context, skill.SynthesisRequest) (*skill.Candidate, error)
}

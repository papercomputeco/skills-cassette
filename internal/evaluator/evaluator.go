// Package evaluator defines the skills-evaluator cassette boundary used by
// asynchronous candidate generation.
package evaluator

import (
	"context"
	"encoding/json"
)

const (
	// GenerationCandidateProfile identifies the immutable skills-cassette-owned
	// criteria set used for candidate selection.
	GenerationCandidateProfile = "generation-candidate-v1"
	// CandidateEvaluationsPath is the tenant-local cassette route.
	CandidateEvaluationsPath = "/v1/cassettes/skills-evaluator/candidate-evaluations"
)

// CandidateBundle is a complete skill snapshot independent of evaluator wire
// representation.
type CandidateBundle struct {
	Slug             string
	Name             string
	Description      string
	Type             string
	Tags             []string
	Content          string
	SourceSessionIDs []string
	ParentID         string
}

// CandidateEvaluationRequest contains one independent candidate and its
// explicit evidence. OwnerSubject is forwarded as trusted routing metadata and
// is never serialized into the evaluator body.
type CandidateEvaluationRequest struct {
	Ref                string
	Name               string
	Candidate          CandidateBundle
	Baseline           *CandidateBundle
	AuthorContext      string
	EvidenceSessionIDs []string
	OwnerSubject       string
	Profile            string
	ProfileVersion     string
	Criteria           []Criterion
}

// CandidateEvaluation is the bounded, evaluator-owned judgment persisted by
// skills-cassette. Structured details remain opaque outside this boundary.
type CandidateEvaluation struct {
	Profile              string
	ProfileVersion       string
	EvaluatorVersion     string
	Score                *float64
	Decision             string
	CriticalFindingCount int
	WarningFindingCount  int
	CriterionResults     json.RawMessage
	Findings             json.RawMessage
	Strengths            json.RawMessage
	Panel                json.RawMessage
}

// CandidateEvaluator judges one complete candidate without mutation.
type CandidateEvaluator interface {
	EvaluateCandidate(context.Context, CandidateEvaluationRequest) (CandidateEvaluation, error)
}

// CallError is a bounded failure classification safe to persist as a curated
// diagnostic. It never wraps or exposes a provider response body.
type CallError struct {
	Code      string
	Message   string
	Retryable bool
	cause     error
}

func (e *CallError) Error() string { return e.Message }

// Unwrap preserves context cancellation semantics without exposing raw
// transport or provider errors through Error.
func (e *CallError) Unwrap() error { return e.cause }

func callError(code, message string, retryable bool, cause error) *CallError {
	return &CallError{Code: code, Message: message, Retryable: retryable, cause: cause}
}

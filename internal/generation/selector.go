// Package generation orchestrates durable asynchronous skill candidate work.
package generation

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
)

// RankedCandidate couples stable candidate identity with its evaluator-owned
// judgment. The selector never invokes a second judge.
type RankedCandidate struct {
	ID         string
	Ordinal    int
	Evaluation evaluator.CandidateEvaluation
}

// RankCandidates returns a deterministically ordered copy, best first. The
// expected profile is the immutable caller-owned profile persisted with the
// generation; selection never substitutes a package-global policy.
func RankCandidates(expectedProfile string, candidates []RankedCandidate) ([]RankedCandidate, error) {
	if expectedProfile == "" {
		return nil, errors.New("generation evaluator profile is required")
	}
	ranked := append([]RankedCandidate(nil), candidates...)
	seen := make(map[string]struct{}, len(ranked))
	for i, candidate := range ranked {
		if err := validateRankedCandidate(expectedProfile, candidate); err != nil {
			return nil, fmt.Errorf("candidate at index %d is not rankable: %w", i, err)
		}
		if _, exists := seen[candidate.ID]; exists {
			return nil, errors.New("candidate identities must be unique")
		}
		seen[candidate.ID] = struct{}{}
	}
	sort.Slice(ranked, func(i, j int) bool { return candidateRanksBefore(ranked[i], ranked[j]) })
	return ranked, nil
}

// SelectWinner returns the highest-ranked candidate for the persisted profile.
func SelectWinner(expectedProfile string, candidates []RankedCandidate) (RankedCandidate, error) {
	ranked, err := RankCandidates(expectedProfile, candidates)
	if err != nil {
		return RankedCandidate{}, err
	}
	if len(ranked) == 0 {
		return RankedCandidate{}, errors.New("no rankable candidates")
	}
	return ranked[0], nil
}

func validateRankedCandidate(expectedProfile string, candidate RankedCandidate) error {
	if candidate.ID == "" {
		return errors.New("identity is required")
	}
	if candidate.Ordinal < 0 {
		return errors.New("ordinal cannot be negative")
	}
	evaluation := candidate.Evaluation
	if evaluation.Profile != expectedProfile {
		return errors.New("evaluation profile does not match generation profile")
	}
	if evaluation.Score == nil || math.IsNaN(*evaluation.Score) || math.IsInf(*evaluation.Score, 0) ||
		*evaluation.Score < 0 || *evaluation.Score > 1 {
		return errors.New("finite score from zero through one is required")
	}
	if evaluation.Decision != "pass" && evaluation.Decision != "revise" {
		return errors.New("decision must be pass or revise")
	}
	if evaluation.CriticalFindingCount < 0 || evaluation.WarningFindingCount < 0 {
		return errors.New("finding counts cannot be negative")
	}
	return nil
}

func candidateRanksBefore(left, right RankedCandidate) bool {
	leftEvaluation := left.Evaluation
	rightEvaluation := right.Evaluation
	if *leftEvaluation.Score != *rightEvaluation.Score {
		return *leftEvaluation.Score > *rightEvaluation.Score
	}
	if leftEvaluation.Decision != rightEvaluation.Decision {
		return leftEvaluation.Decision == "pass"
	}
	if leftEvaluation.CriticalFindingCount != rightEvaluation.CriticalFindingCount {
		return leftEvaluation.CriticalFindingCount < rightEvaluation.CriticalFindingCount
	}
	if leftEvaluation.WarningFindingCount != rightEvaluation.WarningFindingCount {
		return leftEvaluation.WarningFindingCount < rightEvaluation.WarningFindingCount
	}
	if left.Ordinal != right.Ordinal {
		return left.Ordinal < right.Ordinal
	}
	return left.ID < right.ID
}

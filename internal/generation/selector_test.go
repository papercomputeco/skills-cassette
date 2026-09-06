package generation_test

import (
	"fmt"
	"math"
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/generation"
)

func TestGeneration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Generation Suite")
}

func score(value float64) *float64 { return &value }

const persistedSelectorProfile = "caller-owned-ranking-profile"

func ranked(id string, ordinal int, value float64, decision string, critical, warning int) generation.RankedCandidate {
	return generation.RankedCandidate{
		ID: id, Ordinal: ordinal,
		Evaluation: evaluator.CandidateEvaluation{
			Profile: persistedSelectorProfile, Score: score(value), Decision: decision,
			CriticalFindingCount: critical, WarningFindingCount: warning,
		},
	}
}

func candidateIDs(candidates []generation.RankedCandidate) []string {
	ids := make([]string, len(candidates))
	for index := range candidates {
		ids[index] = candidates[index].ID
	}
	return ids
}

var _ = Describe("candidate selector", func() {
	It("selector_ranks_evaluations_deterministically", func() {
		candidates := []generation.RankedCandidate{
			ranked("lower-score", 0, 0.80, "pass", 0, 0),
			ranked("revise", 0, 0.90, "revise", 0, 0),
			ranked("critical", 0, 0.90, "pass", 1, 0),
			ranked("warning", 0, 0.90, "pass", 0, 1),
			ranked("ordinal", 1, 0.90, "pass", 0, 0),
			ranked("lexical-b", 0, 0.90, "pass", 0, 0),
			ranked("lexical-a", 0, 0.90, "pass", 0, 0),
			ranked("high-score", 9, 0.95, "revise", 9, 9),
		}
		expected := []string{
			"high-score", "lexical-a", "lexical-b", "ordinal",
			"warning", "critical", "revise", "lower-score",
		}

		By("exhaustively permuting a set that crosses every documented tie-break")
		permutation := append([]generation.RankedCandidate(nil), candidates...)
		permutationCount := 0
		var firstFailure error
		var visitPermutation func(int)
		visitPermutation = func(index int) {
			if firstFailure != nil {
				return
			}
			if index == len(permutation) {
				permutationCount++
				inputBefore := candidateIDs(permutation)
				rankedCandidates, err := generation.RankCandidates(persistedSelectorProfile, permutation)
				if err != nil {
					firstFailure = fmt.Errorf("permutation %d: %w", permutationCount, err)
					return
				}
				if ids := candidateIDs(rankedCandidates); !slices.Equal(ids, expected) {
					firstFailure = fmt.Errorf("permutation %d ranked %v, want %v", permutationCount, ids, expected)
					return
				}
				if inputAfter := candidateIDs(permutation); !slices.Equal(inputAfter, inputBefore) {
					firstFailure = fmt.Errorf("permutation %d mutated input from %v to %v", permutationCount, inputBefore, inputAfter)
				}
				return
			}
			for swapIndex := index; swapIndex < len(permutation); swapIndex++ {
				permutation[index], permutation[swapIndex] = permutation[swapIndex], permutation[index]
				visitPermutation(index + 1)
				permutation[index], permutation[swapIndex] = permutation[swapIndex], permutation[index]
			}
		}
		visitPermutation(0)
		Expect(firstFailure).NotTo(HaveOccurred())
		Expect(permutationCount).To(Equal(40320), "the property check must cover every 8! input ordering")

		By("isolating every comparator branch")
		for _, tieBreak := range []struct {
			name        string
			left, right generation.RankedCandidate
		}{
			{name: "score descending", left: ranked("score-high", 9, 0.91, "revise", 9, 9), right: ranked("score-low", 0, 0.90, "pass", 0, 0)},
			{name: "pass before revise", left: ranked("decision-pass", 9, 0.90, "pass", 9, 9), right: ranked("decision-revise", 0, 0.90, "revise", 0, 0)},
			{name: "critical findings ascending", left: ranked("critical-zero", 9, 0.90, "pass", 0, 9), right: ranked("critical-one", 0, 0.90, "pass", 1, 0)},
			{name: "warning findings ascending", left: ranked("warning-zero", 9, 0.90, "pass", 0, 0), right: ranked("warning-one", 0, 0.90, "pass", 0, 1)},
			{name: "ordinal ascending", left: ranked("ordinal-zero", 0, 0.90, "pass", 0, 0), right: ranked("ordinal-one", 1, 0.90, "pass", 0, 0)},
			{name: "UUID lexical ascending", left: ranked("00000000-0000-0000-0000-000000000001", 0, 0.90, "pass", 0, 0), right: ranked("00000000-0000-0000-0000-000000000002", 0, 0.90, "pass", 0, 0)},
		} {
			rankedPair, err := generation.RankCandidates(persistedSelectorProfile, []generation.RankedCandidate{tieBreak.right, tieBreak.left})
			Expect(err).NotTo(HaveOccurred(), tieBreak.name)
			Expect(candidateIDs(rankedPair)).To(Equal([]string{tieBreak.left.ID, tieBreak.right.ID}), tieBreak.name)
		}

		winner, err := generation.SelectWinner(persistedSelectorProfile, candidates)
		Expect(err).NotTo(HaveOccurred())
		Expect(winner.ID).To(Equal("high-score"))

		invalid := []generation.RankedCandidate{
			{ID: "missing-score-and-profile", Evaluation: evaluator.CandidateEvaluation{Decision: "pass"}},
			ranked("nan", 0, math.NaN(), "pass", 0, 0),
			ranked("positive-infinite", 0, math.Inf(1), "pass", 0, 0),
			ranked("negative-infinite", 0, math.Inf(-1), "pass", 0, 0),
			ranked("below-range", 0, -0.01, "pass", 0, 0),
			ranked("above-range", 0, 1.01, "pass", 0, 0),
			ranked("unknown-decision", 0, 0.5, "maybe", 0, 0),
			ranked("negative-ordinal", -1, 0.5, "pass", 0, 0),
			ranked("negative-critical", 0, 0.5, "pass", -1, 0),
			ranked("negative-warning", 0, 0.5, "pass", 0, -1),
		}
		for _, candidate := range invalid {
			_, err := generation.RankCandidates(persistedSelectorProfile, []generation.RankedCandidate{candidate})
			Expect(err).To(HaveOccurred(), "expected %s to be unrankable", candidate.ID)
		}
		_, err = generation.RankCandidates(persistedSelectorProfile, []generation.RankedCandidate{
			ranked("duplicate", 0, 0.8, "pass", 0, 0), ranked("duplicate", 1, 0.9, "pass", 0, 0),
		})
		Expect(err).To(MatchError(ContainSubstring("identities must be unique")))
		_, err = generation.SelectWinner(persistedSelectorProfile, nil)
		Expect(err).To(MatchError(ContainSubstring("no rankable candidates")))
		_, err = generation.RankCandidates("", candidates)
		Expect(err).To(MatchError(ContainSubstring("profile is required")))
		_, err = generation.RankCandidates("another-caller-profile", candidates)
		Expect(err).To(MatchError(ContainSubstring("does not match generation profile")))
	})
})

package evaluator_test

import (
	"context"
	"net/http"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
)

var _ = Describe("real cassette contract", func() {
	It("real_cassettes_exchange_candidate_evaluation_contract", func() {
		endpoint := os.Getenv("SKILLS_EVALUATOR_CONTRACT_URL")
		if endpoint == "" {
			Skip("SKILLS_EVALUATOR_CONTRACT_URL is not set")
		}
		criteria := evaluator.GenerationCandidateCriteria()
		client := evaluator.NewHTTPClient(endpoint, &http.Client{Timeout: 10 * time.Second})

		result, err := client.EvaluateCandidate(context.Background(), evaluator.CandidateEvaluationRequest{
			Ref: "generation-contract/candidate-contract", Name: "Contract Candidate",
			Candidate: evaluator.CandidateBundle{
				Slug: "contract-candidate", Name: "Contract Candidate",
				Description: "Use when verifying the cassette contract.", Type: "workflow",
				Content: "## Steps\n\n1. Check the contract.", SourceSessionIDs: []string{"contract-session"},
			},
			Baseline: &evaluator.CandidateBundle{
				Slug: "contract-baseline", Name: "Contract Baseline", Type: "workflow",
				Content: "## Steps\n\n1. Start.",
			},
			AuthorContext:      "Prefer a safe, reusable first draft.",
			EvidenceSessionIDs: []string{"contract-session"}, OwnerSubject: "contract-owner",
			Profile: criteria.Profile, ProfileVersion: criteria.Version, Criteria: criteria.Criteria,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Profile).To(Equal(criteria.Profile))
		Expect(result.ProfileVersion).To(Equal(criteria.Version))
		Expect(result.EvaluatorVersion).To(ContainSubstring("contract"))
		Expect(result.Score).NotTo(BeNil())
		Expect(*result.Score).To(BeNumerically("~", 12.0/14.0, 0.000001))
		Expect(result.Decision).To(Equal("revise"))
		Expect(result.WarningFindingCount).To(Equal(1))
		Expect(result.CriticalFindingCount).To(Equal(1))
		Expect(result.CriterionResults).To(MatchJSON(`[
			{"criterion_id":"intent-alignment","weight":3,"passed":true,"rationale":"intent-alignment passed consistently."},
			{"criterion_id":"evidence-grounding","weight":3,"passed":true,"rationale":"evidence-grounding passed consistently."},
			{"criterion_id":"actionable-workflow","weight":3,"passed":true,"rationale":"actionable-workflow passed consistently."},
			{"criterion_id":"safe-boundaries","weight":2,"passed":false,"rationale":"Add an explicit confirmation before consequential actions."},
			{"criterion_id":"reusable-scope","weight":2,"passed":true,"rationale":"reusable-scope passed consistently."},
			{"criterion_id":"clarity-and-focus","weight":1,"passed":true,"rationale":"clarity-and-focus passed consistently."}
		]`))
		Expect(result.Findings).To(MatchJSON(`[
			{"rule_id":"evidence.support","severity":"critical","message":"The evidence requires an explicit safety check.","file":"","line":0},
			{"rule_id":"safe-boundaries","severity":"warn","message":"Add an explicit confirmation before consequential actions.","file":"","line":0}
		]`))
		Expect(result.Strengths).To(ContainSubstring("intent-alignment passed consistently"))
		Expect(result.Panel).To(MatchJSON(`{"judge_count":1,"aggregation":"single-judge","evidence_sessions_considered":1}`))
	})
})

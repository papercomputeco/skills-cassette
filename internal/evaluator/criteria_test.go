package evaluator_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
)

var _ = Describe("generation ranking criteria", func() {
	It("generation_ranking_criteria_are_stable_and_fit_for_first_drafts", func() {
		criteria := evaluator.GenerationCandidateCriteria()

		Expect(criteria.Profile).To(Equal(evaluator.GenerationCandidateProfile))
		Expect(criteria.Version).To(Equal("1"))
		Expect(criteria.Criteria).To(Equal([]evaluator.Criterion{
			{ID: "intent-alignment", Kind: "content", Weight: 3, Description: "The skill directly addresses the generation goal and author context without contradicting either."},
			{ID: "evidence-grounding", Kind: "content", Weight: 3, Description: "When session evidence is provided, the skill reflects reusable behavior supported by that evidence and does not invent unsupported steps or claims."},
			{ID: "actionable-workflow", Kind: "structure", Weight: 3, Description: "The skill states when to use it and gives an agent concrete, ordered instructions with enough detail to begin the work."},
			{ID: "safe-boundaries", Kind: "content", Weight: 2, Description: "The skill identifies material preconditions, confirmation points, and failure handling needed to avoid unsafe or destructive behavior when applicable."},
			{ID: "reusable-scope", Kind: "content", Weight: 2, Description: "The skill generalizes the workflow instead of copying incidental session details, private identifiers, or one-off outcomes."},
			{ID: "clarity-and-focus", Kind: "structure", Weight: 1, Description: "The skill is coherent, concise, and written as reusable agent instructions rather than transcript or evaluation commentary."},
		}))

		encoded, err := json.Marshal(criteria.Criteria)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).To(Equal(`[{"id":"intent-alignment","kind":"content","description":"The skill directly addresses the generation goal and author context without contradicting either.","weight":3},{"id":"evidence-grounding","kind":"content","description":"When session evidence is provided, the skill reflects reusable behavior supported by that evidence and does not invent unsupported steps or claims.","weight":3},{"id":"actionable-workflow","kind":"structure","description":"The skill states when to use it and gives an agent concrete, ordered instructions with enough detail to begin the work.","weight":3},{"id":"safe-boundaries","kind":"content","description":"The skill identifies material preconditions, confirmation points, and failure handling needed to avoid unsafe or destructive behavior when applicable.","weight":2},{"id":"reusable-scope","kind":"content","description":"The skill generalizes the workflow instead of copying incidental session details, private identifiers, or one-off outcomes.","weight":2},{"id":"clarity-and-focus","kind":"structure","description":"The skill is coherent, concise, and written as reusable agent instructions rather than transcript or evaluation commentary.","weight":1}]`))

		criteria.Criteria[0].Description = "mutated"
		Expect(evaluator.GenerationCandidateCriteria().Criteria[0].Description).NotTo(Equal("mutated"))
	})
})

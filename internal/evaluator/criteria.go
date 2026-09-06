package evaluator

// Criterion is one caller-owned, independently scored expectation.
type Criterion struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Description string `json:"description"`
	Weight      int    `json:"weight"`
}

// CriteriaSet identifies an immutable caller-owned rubric.
type CriteriaSet struct {
	Profile  string
	Version  string
	Criteria []Criterion
}

var generationCandidateCriteriaV1 = []Criterion{
	{ID: "intent-alignment", Kind: "content", Weight: 3, Description: "The skill directly addresses the generation goal and author context without contradicting either."},
	{ID: "evidence-grounding", Kind: "content", Weight: 3, Description: "When session evidence is provided, the skill reflects reusable behavior supported by that evidence and does not invent unsupported steps or claims."},
	{ID: "actionable-workflow", Kind: "structure", Weight: 3, Description: "The skill states when to use it and gives an agent concrete, ordered instructions with enough detail to begin the work."},
	{ID: "safe-boundaries", Kind: "content", Weight: 2, Description: "The skill identifies material preconditions, confirmation points, and failure handling needed to avoid unsafe or destructive behavior when applicable."},
	{ID: "reusable-scope", Kind: "content", Weight: 2, Description: "The skill generalizes the workflow instead of copying incidental session details, private identifiers, or one-off outcomes."},
	{ID: "clarity-and-focus", Kind: "structure", Weight: 1, Description: "The skill is coherent, concise, and written as reusable agent instructions rather than transcript or evaluation commentary."},
}

// GenerationCandidateCriteria returns the skills-cassette-owned rubric used to
// rank candidate drafts. Existing versions are immutable; policy changes add a
// new profile version so in-flight generations retain one consistent baseline.
func GenerationCandidateCriteria() CriteriaSet {
	return CriteriaSet{
		Profile:  GenerationCandidateProfile,
		Version:  "1",
		Criteria: append([]Criterion(nil), generationCandidateCriteriaV1...),
	}
}

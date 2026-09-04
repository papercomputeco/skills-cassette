package evaluator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
)

// The real contract runs two production HTTP applications: this cassette's
// server binary and the skills-evaluator FastAPI app behind its deterministic
// test-only judge. scripts/test-skills-evaluator-contract.sh starts both and
// exports these addresses. Neither side is replaced by a fake HTTP response.
const (
	contractSubjectHeader = "x-paper-auth-subject"
	// contractOwner and contractSessionID are the only owner-scoped evidence
	// the evaluator's deterministic test program answers.
	contractOwner     = "contract-owner"
	contractSessionID = "contract-session"
	contractMember    = "contract-member"
)

type contractEndpoints struct {
	candidateURL  string
	evaluatorBase string
	cassetteBase  string
	client        *http.Client
}

func requireCandidateContract() contractEndpoints {
	endpoint := os.Getenv("SKILLS_EVALUATOR_CONTRACT_URL")
	if endpoint == "" {
		Skip("SKILLS_EVALUATOR_CONTRACT_URL is not set")
	}
	return contractEndpoints{candidateURL: endpoint, client: &http.Client{Timeout: 10 * time.Second}}
}

func requireRevisionContract() contractEndpoints {
	endpoints := requireCandidateContract()
	endpoints.evaluatorBase = strings.TrimRight(os.Getenv("SKILLS_EVALUATOR_CONTRACT_BASE_URL"), "/")
	endpoints.cassetteBase = strings.TrimRight(os.Getenv("SKILLS_CASSETTE_CONTRACT_URL"), "/")
	if endpoints.evaluatorBase == "" || endpoints.cassetteBase == "" {
		Skip("SKILLS_EVALUATOR_CONTRACT_BASE_URL / SKILLS_CASSETTE_CONTRACT_URL are not set")
	}
	return endpoints
}

func (e contractEndpoints) call(method, url, subject string, body any) (int, map[string]any) {
	GinkgoHelper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		Expect(err).NotTo(HaveOccurred())
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(context.Background(), method, url, reader)
	Expect(err).NotTo(HaveOccurred())
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if subject != "" {
		request.Header.Set(contractSubjectHeader, subject)
	}
	response, err := e.client.Do(request)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	Expect(err).NotTo(HaveOccurred())
	decoded := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		Expect(json.Unmarshal(raw, &decoded)).To(Succeed(), "%s %s returned non-JSON %d: %s", method, url, response.StatusCode, raw)
	}
	return response.StatusCode, decoded
}

// Cassette lifecycle calls go to the live cassette's own listener exactly as a
// Console or CLI would; the evaluator reaches the same process through the
// core-shaped prefix its production TapesClient speaks.

func (e contractEndpoints) resolveSkill(subject string) string {
	GinkgoHelper()
	status, skill := e.call(http.MethodPost, e.cassetteBase+"/api/skills", subject,
		map[string]any{"slug": "contract-" + uuid.NewString()[:8]})
	Expect(status).To(Equal(http.StatusOK), "%v", skill)
	return skill["id"].(string)
}

func (e contractEndpoints) appendRevision(subject, skillID, content string, sessions []string) string {
	GinkgoHelper()
	status, revision := e.call(http.MethodPost, e.cassetteBase+"/api/skills/"+skillID+"/revisions", subject,
		map[string]any{
			"snapshot": map[string]any{
				"name": "Contract Revision", "description": "Use when verifying the revision contract.",
				"type": "workflow", "tags": []string{"contract"}, "content": content,
				"isAiGenerated": false, "sourceSessionIds": sessions,
			},
			"idempotencyKey": uuid.NewString(),
		})
	Expect(status).To(Equal(http.StatusCreated), "%v", revision)
	Expect(revision["visibility"]).To(HaveKeyWithValue("isPublic", false), "appended revisions start private")
	return revision["id"].(string)
}

func (e contractEndpoints) setVisibility(subject, skillID, revisionID string, isPublic bool) {
	GinkgoHelper()
	status, body := e.call(http.MethodPut, e.cassetteBase+"/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility",
		subject, map[string]any{"isPublic": isPublic})
	Expect(status).To(Equal(http.StatusOK), "%v", body)
}

func (e contractEndpoints) setLatest(subject, skillID, revisionID string) {
	GinkgoHelper()
	status, body := e.call(http.MethodPut, e.cassetteBase+"/api/skills/"+skillID+"/latest",
		subject, map[string]any{"revisionId": revisionID})
	Expect(status).To(Equal(http.StatusOK), "%v", body)
}

func (e contractEndpoints) evaluate(subject, skillID, revisionID string, candidate *string) (int, map[string]any) {
	GinkgoHelper()
	body := map[string]any{
		"ref":               map[string]any{"source": "skills-cassette", "id": "revision-contract/" + revisionID},
		"skill_id":          skillID,
		"skill_revision_id": revisionID,
	}
	if candidate != nil {
		body["candidate"] = map[string]any{"skill_md": *candidate}
	}
	return e.call(http.MethodPost, e.evaluatorBase+"/evaluate", subject, body)
}

func (e contractEndpoints) listRuns(subject, skillID, revisionID string) (int, map[string]any) {
	GinkgoHelper()
	return e.call(http.MethodGet, e.evaluatorBase+"/evaluation-runs?skill_id="+skillID+"&skill_revision_id="+revisionID, subject, nil)
}

func (e contractEndpoints) getRun(subject, runID string) (int, map[string]any) {
	GinkgoHelper()
	return e.call(http.MethodGet, e.evaluatorBase+"/evaluation-runs/"+runID, subject, nil)
}

func runIDs(list map[string]any) []string {
	GinkgoHelper()
	items, ok := list["items"].([]any)
	Expect(ok).To(BeTrue(), "%v", list)
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.(map[string]any)["id"].(string))
	}
	return ids
}

var _ = Describe("real cassette contract", func() {
	It("real_cassettes_exchange_candidate_and_revision_evaluation_contracts", func() {
		endpoints := requireRevisionContract()

		By("exchanging the stateless candidate contract through the production adapter")
		criteria := evaluator.GenerationCandidateCriteria()
		client := evaluator.NewHTTPClient(endpoints.candidateURL, endpoints.client)
		result, err := client.EvaluateCandidate(context.Background(), evaluator.CandidateEvaluationRequest{
			Ref: "generation-contract/candidate-contract", Name: "Contract Candidate",
			Candidate: evaluator.CandidateBundle{
				Slug: "contract-candidate", Name: "Contract Candidate",
				Description: "Use when verifying the cassette contract.", Type: "workflow",
				Content: "## Steps\n\n1. Check the contract.", SourceSessionIDs: []string{contractSessionID},
			},
			Baseline: &evaluator.CandidateBundle{
				Slug: "contract-baseline", Name: "Contract Baseline", Type: "workflow",
				Content: "## Steps\n\n1. Start.",
			},
			AuthorContext:      "Prefer a safe, reusable first draft.",
			EvidenceSessionIDs: []string{contractSessionID}, OwnerSubject: contractOwner,
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

		By("exchanging the durable revision contract between the two live processes")
		skillID := endpoints.resolveSkill(contractOwner)
		revisionID := endpoints.appendRevision(contractOwner, skillID, "## Steps\n\n1. Check the contract.", []string{contractSessionID})

		status, evaluation := endpoints.evaluate(contractOwner, skillID, revisionID, nil)
		Expect(status).To(Equal(http.StatusOK), "%v", evaluation)
		Expect(evaluation["ref"]).To(HaveKeyWithValue("source", "skills-cassette"))
		Expect(evaluation["ref"]).To(HaveKeyWithValue("id", "revision-contract/"+revisionID))
		Expect(evaluation["evaluator_version"]).To(ContainSubstring("contract"))
		Expect(evaluation["decision"]).To(BeElementOf("pass", "revise"))
		Expect(evaluation["metrics"]).To(HaveKeyWithValue("provenance_sessions", BeNumerically("==", 1)),
			"the evaluator loads the private revision's own provenance session with the trusted viewer subject")

		status, runs := endpoints.listRuns(contractOwner, skillID, revisionID)
		Expect(status).To(Equal(http.StatusOK), "%v", runs)
		Expect(runIDs(runs)).To(HaveLen(1))
		run := runs["items"].([]any)[0].(map[string]any)
		Expect(run).To(HaveKeyWithValue("skill_id", skillID))
		Expect(run).To(HaveKeyWithValue("skill_revision_id", revisionID), "persisted runs carry the exact revision UUID")
		Expect(run).To(HaveKeyWithValue("origin", "manual"))
	})

	It("evaluator_targets_exact_private_or_public_revision_uuid", func() {
		endpoints := requireRevisionContract()
		skillID := endpoints.resolveSkill(contractOwner)
		olderContent := "## Steps\n\n1. Check the contract."
		newerContent := "## Steps\n\n1. Check the contract.\n2. Confirm with the user first."
		older := endpoints.appendRevision(contractOwner, skillID, olderContent, []string{contractSessionID})
		newer := endpoints.appendRevision(contractOwner, skillID, newerContent, []string{contractSessionID})
		endpoints.setVisibility(contractOwner, skillID, older, true)
		endpoints.setVisibility(contractOwner, skillID, newer, true)
		endpoints.setLatest(contractOwner, skillID, newer)

		By("judging the exact requested public revision rather than the explicit latest")
		status, body := endpoints.evaluate(contractOwner, skillID, older, &olderContent)
		Expect(status).To(Equal(http.StatusOK), "%v", body)
		status, body = endpoints.evaluate(contractOwner, skillID, older, &newerContent)
		Expect(status).To(Equal(http.StatusConflict), "%v", body)
		Expect(body["detail"]).To(ContainSubstring("does not match the immutable skill revision"),
			"latest content must not be substituted for the requested revision UUID")

		By("judging a private revision for its creator only")
		private := endpoints.appendRevision(contractOwner, skillID, "## Steps\n\n1. Keep this private.", []string{contractSessionID})
		status, body = endpoints.evaluate(contractOwner, skillID, private, nil)
		Expect(status).To(Equal(http.StatusOK), "%v", body)
		status, body = endpoints.evaluate(contractMember, skillID, private, nil)
		Expect(status).To(Equal(http.StatusNotFound), "%v", body)
		Expect(body["detail"]).NotTo(ContainSubstring("private"), "a guessed private revision behaves as not found")

		By("rejecting revision UUIDs outside the named skill")
		status, body = endpoints.evaluate(contractOwner, skillID, uuid.NewString(), nil)
		Expect(status).To(Equal(http.StatusNotFound), "%v", body)
		otherSkill := endpoints.resolveSkill(contractOwner)
		status, body = endpoints.evaluate(contractOwner, otherSkill, older, nil)
		Expect(status).To(Equal(http.StatusNotFound), "%v", body)
	})

	It("evaluation_history_follows_revision_visibility", func() {
		endpoints := requireRevisionContract()
		skillID := endpoints.resolveSkill(contractOwner)
		revisionID := endpoints.appendRevision(contractOwner, skillID, "## Steps\n\n1. Check the contract.", []string{contractSessionID})
		status, body := endpoints.evaluate(contractOwner, skillID, revisionID, nil)
		Expect(status).To(Equal(http.StatusOK), "%v", body)
		status, runs := endpoints.listRuns(contractOwner, skillID, revisionID)
		Expect(status).To(Equal(http.StatusOK), "%v", runs)
		ids := runIDs(runs)
		Expect(ids).To(HaveLen(1))
		runID := ids[0]

		By("hiding the run from other members while the target revision is private")
		status, body = endpoints.getRun(contractMember, runID)
		Expect(status).To(Equal(http.StatusNotFound), "%v", body)
		status, body = endpoints.listRuns(contractMember, skillID, revisionID)
		Expect(status).To(Equal(http.StatusNotFound), "%v", body)

		By("exposing the same run once the revision is public")
		endpoints.setVisibility(contractOwner, skillID, revisionID, true)
		status, body = endpoints.getRun(contractMember, runID)
		Expect(status).To(Equal(http.StatusOK), "%v", body)
		Expect(body).To(HaveKeyWithValue("skill_revision_id", revisionID))
		status, runs = endpoints.listRuns(contractMember, skillID, revisionID)
		Expect(status).To(Equal(http.StatusOK), "%v", runs)
		Expect(runIDs(runs)).To(Equal([]string{runID}))

		By("hiding it again when the creator makes the revision private")
		endpoints.setVisibility(contractOwner, skillID, revisionID, false)
		status, body = endpoints.getRun(contractMember, runID)
		Expect(status).To(Equal(http.StatusNotFound), "%v", body)
		status, body = endpoints.getRun(contractOwner, runID)
		Expect(status).To(Equal(http.StatusOK), "%v", body)
	})
})

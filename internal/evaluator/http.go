package evaluator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const (
	authSubjectHeader = "x-paper-auth-subject"

	// A legal evaluator request can contain both the generated candidate and
	// the complete baseline at revision maxima. Frontmatter values are JSON
	// encoded once by RenderSkillMD and then again in the request (at most seven
	// bytes per rune); content and ancillary values are encoded once (at most
	// six). These fixed terms cover the exact field maxima plus bounded framing,
	// criteria, context, and evidence collections without making transport
	// unbounded.
	maxEvaluatorEscapedBundleBytes = 7*(storage.MaxRevisionNameCodePoints+
		storage.MaxRevisionDescriptionCodePoints+
		storage.MaxRevisionTags*storage.MaxRevisionIdentityCodePoints+
		storage.MaxRevisionIdentityCodePoints+
		storage.MaxRevisionSourceSessionIDs*storage.MaxRevisionIdentityCodePoints) +
		6*storage.MaxRevisionContentCodePoints + 1024
	maxEvaluatorEscapedAncillaryBytes = 6*storage.MaxGenerationAuthorContextRunes +
		6*storage.MaxRevisionNameCodePoints +
		6*storage.MaxRevisionSourceSessionIDs*storage.MaxRevisionIdentityCodePoints +
		6*3*storage.MaxRevisionIdentityCodePoints + 100*(2*4000*6+256) + 64<<10
	maxEvaluatorRequestBytes      = 2*maxEvaluatorEscapedBundleBytes + maxEvaluatorEscapedAncillaryBytes
	maxEvaluatorResponseBytes     = 1 << 20
	maxEvaluatorDetailsBytes      = 256 << 10
	maxEvaluatorPanelBytes        = 64 << 10
	maxEvaluatorFindings          = 50
	maxEvaluatorCriteria          = 100
	maxEvaluatorStrengths         = 50
	maxEvaluatorTextBytes         = 4000
	maxEvaluatorVersionCodePoints = 256
	maxEvaluatorRefCodePoints     = 256
)

// HTTPClient translates the internal candidate-evaluation domain into the
// stateless skills-evaluator cassette wire contract.
type HTTPClient struct {
	endpoint string
	client   *http.Client
}

// NewHTTPClient creates a production evaluator adapter. endpoint must be the
// complete candidate-evaluations URL.
func NewHTTPClient(endpoint string, client *http.Client) *HTTPClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPClient{endpoint: strings.TrimSpace(endpoint), client: client}
}

// EvaluateCandidate performs one stateless evaluator call.
func (c *HTTPClient) EvaluateCandidate(ctx context.Context, request CandidateEvaluationRequest) (CandidateEvaluation, error) {
	if err := validateEvaluationRequest(request); err != nil {
		return CandidateEvaluation{}, callError("evaluator_invalid_request", "The candidate evaluation request is invalid or too large.", false, nil)
	}
	body, err := json.Marshal(candidateEvaluationRequestWire{
		Ref:                evaluationRefWire{Source: "skills-cassette", ID: request.Ref},
		Name:               request.Name,
		Candidate:          bundleWire(request.Candidate),
		Baseline:           optionalBundleWire(request.Baseline),
		AuthorContext:      request.AuthorContext,
		EvidenceSessionIDs: nonNilStrings(request.EvidenceSessionIDs),
		Profile:            request.Profile,
		ProfileVersion:     request.ProfileVersion,
		Criteria:           append([]Criterion(nil), request.Criteria...),
	})
	if err != nil || len(body) > maxEvaluatorRequestBytes {
		return CandidateEvaluation{}, callError("evaluator_invalid_request", "The candidate evaluation request is invalid or too large.", false, nil)
	}
	if c.endpoint == "" {
		return CandidateEvaluation{}, callError("evaluator_not_configured", "Candidate evaluation is not configured.", false, nil)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return CandidateEvaluation{}, callError("evaluator_not_configured", "Candidate evaluation is not configured.", false, nil)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if request.OwnerSubject != "" {
		httpRequest.Header.Set(authSubjectHeader, request.OwnerSubject)
	}
	response, err := c.client.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return CandidateEvaluation{}, callError("evaluator_canceled", "Candidate evaluation was canceled.", false, ctx.Err())
		}
		return CandidateEvaluation{}, callError("evaluator_unavailable", "Candidate evaluation is temporarily unavailable.", true, nil)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxEvaluatorResponseBytes+1))
		return CandidateEvaluation{}, evaluatorStatusError(response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxEvaluatorResponseBytes+1))
	if err != nil {
		return CandidateEvaluation{}, callError("evaluator_unavailable", "Candidate evaluation response could not be read.", true, nil)
	}
	if len(responseBody) > maxEvaluatorResponseBytes {
		return CandidateEvaluation{}, invalidResponseError()
	}
	var wire candidateEvaluationResponseWire
	if err := strictDecodeEvaluatorJSON(responseBody, &wire); err != nil {
		return CandidateEvaluation{}, invalidResponseError()
	}
	result, err := translateEvaluationResponse(wire, request)
	if err != nil {
		return CandidateEvaluation{}, invalidResponseError()
	}
	return result, nil
}

func validateEvaluationRequest(request CandidateEvaluationRequest) error {
	if request.Ref == "" || strings.TrimSpace(request.Profile) == "" || request.Profile != strings.TrimSpace(request.Profile) ||
		strings.TrimSpace(request.ProfileVersion) == "" || request.ProfileVersion != strings.TrimSpace(request.ProfileVersion) ||
		len(request.Criteria) == 0 || len(request.Criteria) > maxEvaluatorCriteria {
		return errors.New("invalid candidate evaluation identity")
	}
	seen := make(map[string]struct{}, len(request.Criteria))
	for _, criterion := range request.Criteria {
		if criterion.ID == "" || criterion.ID != strings.TrimSpace(criterion.ID) || len(criterion.ID) > maxEvaluatorTextBytes ||
			criterion.Description == "" || criterion.Description != strings.TrimSpace(criterion.Description) || len(criterion.Description) > maxEvaluatorTextBytes ||
			criterion.Weight < 1 || criterion.Weight > 3 {
			return errors.New("invalid candidate evaluation criterion")
		}
		if _, exists := seen[criterion.ID]; exists {
			return errors.New("duplicate candidate evaluation criterion")
		}
		seen[criterion.ID] = struct{}{}
	}
	return nil
}

func evaluatorStatusError(status int) error {
	switch {
	case status == http.StatusTooManyRequests:
		return callError("evaluator_rate_limited", "Candidate evaluation is temporarily rate limited.", true, nil)
	case status == http.StatusRequestTimeout || status >= http.StatusInternalServerError:
		return callError("evaluator_unavailable", "Candidate evaluation is temporarily unavailable.", true, nil)
	default:
		return callError("evaluator_rejected", "The evaluator rejected the candidate evaluation request.", false, nil)
	}
}

func invalidResponseError() error {
	return callError("evaluator_invalid_response", "The evaluator returned an invalid candidate evaluation response.", false, nil)
}

func translateEvaluationResponse(wire candidateEvaluationResponseWire, request CandidateEvaluationRequest) (CandidateEvaluation, error) {
	if wire.Ref.Source != "skills-cassette" || wire.Ref.ID != request.Ref ||
		!validEvaluatorIdentity(wire.Ref.Revision, maxEvaluatorRefCodePoints) ||
		!validEvaluatorIdentity(wire.Ref.RevisionSHA256, maxEvaluatorRefCodePoints) ||
		wire.Profile != request.Profile || wire.ProfileVersion != request.ProfileVersion ||
		wire.Profile == "" || wire.ProfileVersion == "" || !validEvaluatorVersion(wire.EvaluatorVersion) {
		return CandidateEvaluation{}, errors.New("missing or invalid evaluator identity")
	}
	if wire.Score == nil || math.IsNaN(*wire.Score) || math.IsInf(*wire.Score, 0) || *wire.Score < 0 || *wire.Score > 1 {
		return CandidateEvaluation{}, errors.New("invalid evaluator score")
	}
	if wire.Decision != "pass" && wire.Decision != "revise" {
		return CandidateEvaluation{}, errors.New("invalid evaluator decision")
	}
	criteria, err := boundedCriterionResults(wire.CriterionResults, request.Criteria)
	if err != nil {
		return CandidateEvaluation{}, err
	}
	findings, critical, warning, err := boundedFindings(wire.Findings)
	if err != nil {
		return CandidateEvaluation{}, err
	}
	strengths, err := boundedStrengths(wire.Strengths)
	if err != nil {
		return CandidateEvaluation{}, err
	}
	panel, err := boundedJSONObject(wire.Panel, maxEvaluatorPanelBytes)
	if err != nil {
		return CandidateEvaluation{}, err
	}
	return CandidateEvaluation{
		Profile: wire.Profile, ProfileVersion: wire.ProfileVersion, EvaluatorVersion: wire.EvaluatorVersion,
		Score: wire.Score, Decision: wire.Decision,
		CriticalFindingCount: critical, WarningFindingCount: warning,
		CriterionResults: criteria, Findings: findings, Strengths: strengths, Panel: panel,
	}, nil
}

type criterionResultDetail struct {
	CriterionID string `json:"criterion_id"`
	Weight      int    `json:"weight"`
	Passed      bool   `json:"passed"`
	Rationale   string `json:"rationale"`
}

type findingDetail struct {
	RuleID   *string `json:"rule_id,omitempty"`
	Severity string  `json:"severity"`
	Message  string  `json:"message"`
	File     *string `json:"file,omitempty"`
	Line     *int    `json:"line,omitempty"`
}

func validEvaluatorVersion(value string) bool {
	return value != "" && value == strings.TrimSpace(value) &&
		validEvaluatorIdentity(value, maxEvaluatorVersionCodePoints)
}

func validEvaluatorIdentity(value string, maximum int) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maximum {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validEvaluatorText(value string, maximum int) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maximum {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) && character != '\n' && character != '\r' && character != '\t' {
			return false
		}
	}
	return true
}

func boundedCriterionResults(raw json.RawMessage, expected []Criterion) (json.RawMessage, error) {
	items, err := rawJSONArray(raw)
	if err != nil || len(items) != len(expected) || len(items) > maxEvaluatorCriteria || len(raw) > maxEvaluatorDetailsBytes {
		return nil, errors.New("invalid criterion results")
	}
	results := make([]criterionResultDetail, len(items))
	for index, item := range items {
		var wire struct {
			CriterionID *string `json:"criterion_id"`
			Weight      *int    `json:"weight"`
			Passed      *bool   `json:"passed"`
			Rationale   *string `json:"rationale"`
		}
		if strictDecodeDetail(item, &wire) != nil || wire.CriterionID == nil || wire.Weight == nil ||
			wire.Passed == nil || wire.Rationale == nil || *wire.CriterionID != expected[index].ID ||
			*wire.Weight != expected[index].Weight || strings.TrimSpace(*wire.Rationale) == "" ||
			!validEvaluatorText(*wire.Rationale, maxEvaluatorTextBytes) {
			return nil, errors.New("invalid criterion result")
		}
		results[index] = criterionResultDetail{
			CriterionID: *wire.CriterionID, Weight: *wire.Weight,
			Passed: *wire.Passed, Rationale: *wire.Rationale,
		}
	}
	return marshalEvaluatorDetails(results)
}

func boundedFindings(raw json.RawMessage) (json.RawMessage, int, int, error) {
	items, err := rawJSONArray(raw)
	if err != nil || len(items) > maxEvaluatorFindings || len(raw) > maxEvaluatorDetailsBytes {
		return nil, 0, 0, errors.New("invalid findings")
	}
	critical := 0
	warning := 0
	findings := make([]findingDetail, len(items))
	for index, item := range items {
		var finding findingDetail
		if strictDecodeDetail(item, &finding) != nil || strings.TrimSpace(finding.Message) == "" ||
			!validEvaluatorText(finding.Message, maxEvaluatorTextBytes) {
			return nil, 0, 0, errors.New("invalid finding")
		}
		finding.Severity = strings.ToLower(finding.Severity)
		switch finding.Severity {
		case "critical":
			critical++
		case "warn", "warning":
			warning++
		case "info":
		default:
			return nil, 0, 0, errors.New("invalid finding severity")
		}
		if finding.RuleID != nil && !validEvaluatorIdentity(*finding.RuleID, 256) {
			return nil, 0, 0, errors.New("invalid finding rule id")
		}
		if finding.File != nil && !validEvaluatorText(*finding.File, 64<<10) {
			return nil, 0, 0, errors.New("invalid finding file")
		}
		if finding.Line != nil && *finding.Line < 0 {
			return nil, 0, 0, errors.New("invalid finding line")
		}
		findings[index] = finding
	}
	encoded, err := marshalEvaluatorDetails(findings)
	return encoded, critical, warning, err
}

func boundedStrengths(values []string) (json.RawMessage, error) {
	if len(values) > maxEvaluatorStrengths {
		return nil, errors.New("too many strengths")
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || !validEvaluatorText(value, maxEvaluatorTextBytes) {
			return nil, errors.New("invalid strength")
		}
	}
	if values == nil {
		values = []string{}
	}
	encoded, err := marshalEvaluatorDetails(values)
	if err != nil {
		return nil, errors.New("invalid strengths")
	}
	return encoded, nil
}

func rawJSONArray(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []json.RawMessage{}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func boundedJSONObject(raw json.RawMessage, maxBytes int) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage(`{}`), nil
	}
	if len(raw) > maxBytes {
		return nil, errors.New("structured object too large")
	}
	if err := storage.ValidateUniqueJSONObjectFields(raw); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("invalid structured object")
	}
	return raw, nil
}

func strictDecodeDetail(raw json.RawMessage, output any) error {
	return strictDecodeEvaluatorJSON(raw, output)
}

func strictDecodeEvaluatorJSON(raw []byte, output any) error {
	if err := storage.ValidateUniqueJSONObjectFields(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple evaluator JSON values")
		}
		return err
	}
	return nil
}

func marshalEvaluatorDetails(value any) (json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxEvaluatorDetailsBytes {
		return nil, errors.New("invalid evaluator details")
	}
	return encoded, nil
}

func bundleWire(bundle CandidateBundle) candidateBundleWire {
	return candidateBundleWire{SkillMD: renderBundle(bundle)}
}

func optionalBundleWire(bundle *CandidateBundle) *candidateBundleWire {
	if bundle == nil {
		return nil
	}
	wire := bundleWire(*bundle)
	return &wire
}

func renderBundle(bundle CandidateBundle) string {
	name := bundle.Name
	if name == "" {
		name = bundle.Slug
	}
	return skill.RenderSkillMD(&skill.Skill{
		Name: name, Description: bundle.Description, Version: "0.1.0", Tags: nonNilStrings(bundle.Tags),
		Type: bundle.Type, Content: bundle.Content, Sessions: nonNilStrings(bundle.SourceSessionIDs),
	})
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// evaluationRefWire is the evaluator's opaque correlation record. Candidate
// evaluation identifies work by source and id only; the evaluator echoes the
// whole record, including the durable-revision fields it also carries, so those
// are accepted as bounded opaque strings and never interpreted.
type evaluationRefWire struct {
	Source         string `json:"source"`
	ID             string `json:"id"`
	Revision       string `json:"revision,omitempty"`
	RevisionSHA256 string `json:"revision_sha256,omitempty"`
}

type candidateBundleWire struct {
	SkillMD string `json:"skill_md"`
}

type candidateEvaluationRequestWire struct {
	Ref                evaluationRefWire    `json:"ref"`
	Name               string               `json:"name"`
	Candidate          candidateBundleWire  `json:"candidate"`
	Baseline           *candidateBundleWire `json:"baseline,omitempty"`
	AuthorContext      string               `json:"author_context"`
	EvidenceSessionIDs []string             `json:"evidence_session_ids"`
	Profile            string               `json:"profile"`
	ProfileVersion     string               `json:"profile_version"`
	Criteria           []Criterion          `json:"criteria"`
}

type candidateEvaluationResponseWire struct {
	Ref              evaluationRefWire `json:"ref"`
	Profile          string            `json:"profile"`
	ProfileVersion   string            `json:"profile_version"`
	EvaluatorVersion string            `json:"evaluator_version"`
	Score            *float64          `json:"score"`
	Decision         string            `json:"decision"`
	CriterionResults json.RawMessage   `json:"criterion_results"`
	Findings         json.RawMessage   `json:"findings"`
	Strengths        []string          `json:"strengths"`
	Panel            json.RawMessage   `json:"panel"`
}

var _ CandidateEvaluator = (*HTTPClient)(nil)

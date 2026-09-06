package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

const (
	maxGenerationAuthorContextCodePoints = storage.MaxGenerationAuthorContextRunes
	maxGenerationSelectedSessions        = storage.MaxRevisionSourceSessionIDs
)

// generationRevisionContentRequest is complete revision content for one
// generation seed. Stable slug identity comes only from {skillId}; revision
// ancestry comes only from baseRevisionId, so neither slug nor parentId is an
// accepted generation-input field.
type generationRevisionContentRequest struct {
	Name             *string   `json:"name"`
	Description      *string   `json:"description"`
	Type             *string   `json:"type"`
	Tags             *[]string `json:"tags"`
	Content          *string   `json:"content"`
	IsAIGenerated    *bool     `json:"isAiGenerated"`
	SourceSessionIDs *[]string `json:"sourceSessionIds"`
}

type generationRevisionContentResponse struct {
	Name             string   `json:"name"`
	Description      string   `json:"description"`
	Type             string   `json:"type"`
	Tags             []string `json:"tags"`
	Content          string   `json:"content"`
	IsAIGenerated    bool     `json:"isAiGenerated"`
	SourceSessionIDs []string `json:"sourceSessionIds"`
}

type createSkillGenerationRequest struct {
	BaseRevisionID     *string                          `json:"baseRevisionId"`
	Input              generationRevisionContentRequest `json:"input"`
	AuthorContext      *string                          `json:"authorContext"`
	SelectedSessionIDs *[]string                        `json:"selectedSessionIds"`
}

// skillGenerationResponse deliberately omits creator and worker claim data,
// raw evaluator request identities, transcripts, and provider errors.
type skillGenerationResponse struct {
	ID                      string                             `json:"id"`
	SkillID                 string                             `json:"skillId"`
	BaseRevisionID          *string                            `json:"baseRevisionId"`
	Status                  storage.GenerationStatus           `json:"status"`
	Input                   generationRevisionContentResponse  `json:"input"`
	AuthorContext           string                             `json:"authorContext"`
	SelectedSessionIDs      []string                           `json:"selectedSessionIds"`
	SourceSessionIDs        []string                           `json:"sourceSessionIds"`
	EvaluatorProfile        string                             `json:"evaluatorProfile"`
	EvaluatorProfileVersion string                             `json:"evaluatorProfileVersion"`
	EvaluationCriteria      json.RawMessage                    `json:"evaluationCriteria"`
	WinnerCandidateID       *string                            `json:"winnerCandidateId"`
	ResultCandidateID       *string                            `json:"resultCandidateId"`
	ResultRevisionID        *string                            `json:"resultRevisionId"`
	Failure                 *generationFailureResponse         `json:"failure"`
	Sessions                []generationSessionResponse        `json:"sessions"`
	Candidates              []skillGenerationCandidateResponse `json:"candidates"`
	Evaluations             []candidateEvaluationResponse      `json:"evaluations"`
	Diagnostics             []generationDiagnosticResponse     `json:"diagnostics"`
	AttemptCount            int                                `json:"attemptCount"`
	CreatedAt               string                             `json:"createdAt"`
	UpdatedAt               string                             `json:"updatedAt"`
	StartedAt               *string                            `json:"startedAt"`
	CompletedAt             *string                            `json:"completedAt"`
}

type skillGenerationCandidateResponse struct {
	ID               string                            `json:"id"`
	Ordinal          int                               `json:"ordinal"`
	Kind             storage.GenerationCandidateKind   `json:"kind"`
	SourceSessionIDs []string                          `json:"sourceSessionIds"`
	Snapshot         generationRevisionContentResponse `json:"snapshot"`
	Insights         json.RawMessage                   `json:"insights"`
	BundleSHA256     string                            `json:"bundleSha256"`
}

type skillGenerationSummaryResponse struct {
	ID               string                     `json:"id"`
	SkillID          string                     `json:"skillId"`
	BaseRevisionID   *string                    `json:"baseRevisionId"`
	Status           storage.GenerationStatus   `json:"status"`
	ResultRevisionID *string                    `json:"resultRevisionId"`
	Failure          *generationFailureResponse `json:"failure"`
	AttemptCount     int                        `json:"attemptCount"`
	CreatedAt        string                     `json:"createdAt"`
	UpdatedAt        string                     `json:"updatedAt"`
	StartedAt        *string                    `json:"startedAt"`
	CompletedAt      *string                    `json:"completedAt"`
}

type skillGenerationsResponse struct {
	Generations []skillGenerationSummaryResponse `json:"generations"`
	NextCursor  string                           `json:"nextCursor,omitempty"`
}

type skillGenerationCursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

func encodeSkillGenerationCursor(cursor skillGenerationCursor) string {
	value, err := json.Marshal(cursor)
	if err != nil {
		panic(fmt.Sprintf("encode skill generation cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(value)
}

func decodeSkillGenerationCursor(value string) (skillGenerationCursor, error) {
	if value == "" || len(value) > maxSkillCursorCodePoints {
		return skillGenerationCursor{}, errors.New("invalid generation cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return skillGenerationCursor{}, fmt.Errorf("invalid generation cursor: %w", err)
	}
	var cursor skillGenerationCursor
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cursor); err != nil {
		return skillGenerationCursor{}, fmt.Errorf("invalid generation cursor: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return skillGenerationCursor{}, errors.New("invalid generation cursor")
	}
	canonicalID, parseErr := canonicalUUID(cursor.ID)
	if cursor.CreatedAt.IsZero() || parseErr != nil {
		return skillGenerationCursor{}, errors.New("invalid generation cursor")
	}
	cursor.ID = canonicalID
	return cursor, nil
}

func (s *Server) handleCreateGeneration(w http.ResponseWriter, r *http.Request) {
	creator, ok := lifecycleOwner(w, r)
	if !ok {
		return
	}
	if !s.requireGenerationStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}

	var request createSkillGenerationRequest
	if err := decodeStrictGenerationJSONBody(w, r, &request); err != nil {
		writeLifecycleError(w, http.StatusBadRequest, "invalid_request", "The request body is invalid.", "", nil)
		return
	}
	baseRevisionID, validBase := requestRevisionID(request.BaseRevisionID)
	if !validBase {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "baseRevisionId must be a UUID or null.", "")
		return
	}
	snapshot, message := request.Input.snapshot()
	if message != "" {
		writeLifecycleError(w, http.StatusBadRequest, "invalid_request", message, "", nil)
		return
	}
	if request.AuthorContext == nil || request.SelectedSessionIDs == nil {
		writeLifecycleError(w, http.StatusBadRequest, "invalid_request", "authorContext and selectedSessionIds are required.", "", nil)
		return
	}
	selected, message := validateSelectedSessionIDs(*request.SelectedSessionIDs)
	if message != "" || len(selected) > maxGenerationSelectedSessions {
		if message == "" {
			message = "Too many selectedSessionIds were supplied."
		}
		writeLifecycleError(w, http.StatusBadRequest, "invalid_request", message, "", nil)
		return
	}
	authorContext := strings.TrimSpace(*request.AuthorContext)
	if !validBoundedText(*request.AuthorContext, maxGenerationAuthorContextCodePoints) ||
		!validBoundedText(authorContext, maxGenerationAuthorContextCodePoints) {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "The authorContext is invalid or too long.", "")
		return
	}

	criteriaSet := evaluator.GenerationCandidateCriteria()
	criteria, err := json.Marshal(criteriaSet.Criteria)
	if err != nil {
		s.writeGenerationInternalError(w, "snapshot generation criteria", err)
		return
	}
	createdAt := time.Now().UTC()
	generation, err := s.generationStore.CreateGeneration(r.Context(), storage.CreateGenerationInput{
		ID: uuid.NewString(), SkillID: skillID, BaseRevisionID: baseRevisionID,
		CreatorSubject: creator, Snapshot: snapshot, AuthorContext: authorContext,
		SelectedSessionIDs: selected, EvaluatorProfile: criteriaSet.Profile,
		EvaluatorProfileVersion: criteriaSet.Version, EvaluationCriteria: criteria,
		CreatedAt: createdAt,
	})
	if err != nil {
		s.writeGenerationStorageError(w, err)
		return
	}
	if generation == nil {
		s.writeGenerationInternalError(w, "create generation", errors.New("generation store returned no generation"))
		return
	}
	state := storage.GenerationState{Generation: *generation}
	state.Sessions = make([]storage.GenerationSessionRecord, len(selected))
	for ordinal, sessionID := range selected {
		state.Sessions[ordinal] = storage.GenerationSessionRecord{
			GenerationID: generation.ID, SessionID: sessionID, Ordinal: ordinal,
			Status: storage.GenerationSessionPending, UpdatedAt: generation.CreatedAt,
		}
	}
	// CreateGeneration has committed the row and all ordered sessions before the
	// first response write. No post-commit operation uses the cancelable request
	// context, so disconnecting cannot turn accepted durable work into an error.
	writeJSON(w, http.StatusAccepted, skillGenerationWire(state))
}

func (s *Server) handleListGenerations(w http.ResponseWriter, r *http.Request) {
	creator, ok := lifecycleOwner(w, r)
	if !ok {
		return
	}
	if !s.requireGenerationStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	query := r.URL.Query()
	for _, name := range []string{"limit", "cursor"} {
		if len(query[name]) > 1 {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, name+" must be supplied at most once.", "")
			return
		}
	}
	opts := storage.SkillGenerationListOpts{
		CallerSubject: creator, SkillID: skillID, Limit: storage.DefaultGenerationListLimit,
	}
	if values, present := query["limit"]; present {
		limit, err := strconv.Atoi(strings.TrimSpace(values[0]))
		if err != nil || limit < 1 || limit > storage.MaxGenerationListLimit {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "limit must be an integer from 1 through 100.", "")
			return
		}
		opts.Limit = limit
	}
	if values, present := query["cursor"]; present {
		raw := strings.TrimSpace(values[0])
		cursor, err := decodeSkillGenerationCursor(raw)
		if err != nil {
			writeLifecycleError(w, http.StatusBadRequest, "invalid_request", "The generation cursor is invalid.", "", nil)
			return
		}
		opts.CursorCreatedAt = &cursor.CreatedAt
		opts.CursorID = cursor.ID
	}
	page, err := s.generationStore.ListSkillGenerations(r.Context(), opts)
	if err != nil {
		s.writeGenerationStorageError(w, err)
		return
	}
	responses := make([]skillGenerationSummaryResponse, len(page.Generations))
	for i, generation := range page.Generations {
		responses[i] = skillGenerationSummaryWire(generation)
	}
	response := skillGenerationsResponse{Generations: responses}
	if page.NextCreatedAt != nil {
		response.NextCursor = encodeSkillGenerationCursor(skillGenerationCursor{
			CreatedAt: *page.NextCreatedAt, ID: page.NextID,
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetGeneration(w http.ResponseWriter, r *http.Request) {
	creator, ok := lifecycleOwner(w, r)
	if !ok {
		return
	}
	if !s.requireGenerationStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeGenerationNotFound, "The requested generation was not found.", "")
		return
	}
	generationID, err := canonicalUUID(r.PathValue("generationId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeGenerationNotFound, "The requested generation was not found.", "")
		return
	}
	state, err := s.generationStore.GetSkillGeneration(r.Context(), creator, skillID, generationID)
	if err != nil {
		s.writeGenerationStorageError(w, err)
		return
	}
	if state == nil {
		writeLifecycleError(w, http.StatusNotFound, "generation_not_found", "The requested generation was not found.", "", nil)
		return
	}
	writeJSON(w, http.StatusOK, skillGenerationWire(*state))
}

func (s *Server) handleGetGenerationByID(w http.ResponseWriter, r *http.Request) {
	creator, ok := lifecycleOwner(w, r)
	if !ok {
		return
	}
	if !s.requireGenerationStore(w) {
		return
	}
	generationID, err := canonicalUUID(r.PathValue("generationId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeGenerationNotFound, "The requested generation was not found.", "")
		return
	}
	state, err := s.generationStore.GetGenerationByID(r.Context(), creator, generationID)
	if err != nil {
		s.writeGenerationStorageError(w, err)
		return
	}
	if state == nil {
		writeLifecycleError(w, http.StatusNotFound, "generation_not_found", "The requested generation was not found.", "", nil)
		return
	}
	writeJSON(w, http.StatusOK, skillGenerationWire(*state))
}

func (s *Server) handleCancelGeneration(w http.ResponseWriter, r *http.Request) {
	creator, ok := lifecycleOwner(w, r)
	if !ok {
		return
	}
	if !s.requireGenerationStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeGenerationNotFound, "The requested generation was not found.", "")
		return
	}
	generationID, err := canonicalUUID(r.PathValue("generationId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeGenerationNotFound, "The requested generation was not found.", "")
		return
	}
	state, err := s.generationStore.CancelSkillGeneration(r.Context(), creator, skillID, generationID)
	if err != nil {
		s.writeGenerationStorageError(w, err)
		return
	}
	if state == nil {
		s.writeGenerationInternalError(w, "cancel generation", errors.New("generation store returned no canceled generation"))
		return
	}
	writeJSON(w, http.StatusOK, skillGenerationWire(*state))
}

func decodeStrictGenerationJSONBody(w http.ResponseWriter, r *http.Request, out any) error {
	return decodeLifecycleJSONBody(w, r, out)
}

func (request generationRevisionContentRequest) snapshot() (storage.SkillRevisionSnapshot, string) {
	snapshot, err := (revisionSnapshotRequest(request)).snapshot()
	if err != nil {
		return storage.SkillRevisionSnapshot{}, validationMessage(err)
	}
	return snapshot, ""
}

func (s *Server) requireGenerationStore(w http.ResponseWriter) bool {
	if s.generationStore != nil {
		return true
	}
	writeLifecycleError(w, http.StatusNotImplemented, errorCodePersistenceNotConfigured, "Generation persistence is not configured.", "")
	return false
}

func (s *Server) writeGenerationStorageError(w http.ResponseWriter, err error) {
	s.writeLifecycleStorageError(w, "change generation", err)
}

func (s *Server) writeGenerationInternalError(w http.ResponseWriter, operation string, err error) {
	s.logger.Error(operation, "error", err)
	writeLifecycleError(w, http.StatusInternalServerError, errorCodeInternal, "The request could not be completed.", "")
}

func skillGenerationSummaryWire(generation storage.SkillGenerationSummaryRecord) skillGenerationSummaryResponse {
	response := skillGenerationSummaryResponse{
		ID: generation.ID, SkillID: generation.SkillID,
		BaseRevisionID: optionalString(generation.BaseRevisionID), Status: generation.Status,
		ResultRevisionID: optionalString(generation.ResultRevisionID),
		AttemptCount:     generation.AttemptCount,
		CreatedAt:        generation.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:        generation.UpdatedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:        optionalTime(generation.StartedAt),
		CompletedAt:      optionalTime(generation.CompletedAt),
	}
	if generation.ErrorCode != "" || generation.ErrorMessage != "" {
		response.Failure = &generationFailureResponse{
			Code:    boundedLifecycleText(generation.ErrorCode, maxLifecycleCodeCodePoints),
			Message: boundedLifecycleText(generation.ErrorMessage, maxLifecycleMessageCodePoints),
		}
	}
	return response
}

func skillGenerationWire(state storage.GenerationState) skillGenerationResponse {
	generation := state.Generation
	response := skillGenerationResponse{
		ID: generation.ID, SkillID: generation.SkillID,
		BaseRevisionID: optionalString(generation.BaseRevisionID), Status: generation.Status,
		Input:                   generationRevisionContentWire(generation.Snapshot),
		AuthorContext:           generation.AuthorContext,
		SelectedSessionIDs:      nonNilWireStrings(generation.SelectedSessionIDs),
		SourceSessionIDs:        effectiveSourceSessionIDs(state.Candidates),
		EvaluatorProfile:        generation.EvaluatorProfile,
		EvaluatorProfileVersion: generation.EvaluatorProfileVersion,
		EvaluationCriteria:      boundedJSON(generation.EvaluationCriteria, json.RawMessage(`[]`)),
		WinnerCandidateID:       optionalString(generation.WinnerCandidateID),
		ResultCandidateID:       optionalString(generation.ResultCandidateID),
		ResultRevisionID:        optionalString(generation.ResultRevisionID),
		AttemptCount:            generation.AttemptCount,
		CreatedAt:               generation.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:               generation.UpdatedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:               optionalTime(generation.StartedAt), CompletedAt: optionalTime(generation.CompletedAt),
	}
	if generation.ErrorCode != "" || generation.ErrorMessage != "" {
		response.Failure = &generationFailureResponse{
			Code:    boundedLifecycleText(generation.ErrorCode, maxLifecycleCodeCodePoints),
			Message: boundedLifecycleText(generation.ErrorMessage, maxLifecycleMessageCodePoints),
		}
	}
	response.Sessions = make([]generationSessionResponse, len(state.Sessions))
	for i, session := range state.Sessions {
		response.Sessions[i] = generationSessionResponse{
			SessionID: session.SessionID, Ordinal: session.Ordinal, Status: session.Status,
			CandidateID:    optionalString(session.CandidateID),
			DiagnosticCode: optionalString(boundedLifecycleText(session.DiagnosticCode, maxLifecycleCodeCodePoints)),
		}
	}
	candidates := state.Candidates
	if len(candidates) > maxGenerationCandidates {
		candidates = candidates[:maxGenerationCandidates]
	}
	response.Candidates = make([]skillGenerationCandidateResponse, len(candidates))
	for i, candidate := range candidates {
		response.Candidates[i] = skillGenerationCandidateResponse{
			ID: candidate.ID, Ordinal: candidate.Ordinal, Kind: candidate.Kind,
			SourceSessionIDs: nonNilWireStrings(candidate.SourceSessionIDs),
			Snapshot: generationRevisionContentWire(storage.SkillRevisionSnapshot{
				Name: candidate.Snapshot.Name, Description: candidate.Snapshot.Description,
				Type: candidate.Snapshot.Type, Tags: candidate.Snapshot.Tags,
				Content: candidate.Snapshot.Content, IsAIGenerated: candidate.Snapshot.IsAIGenerated,
				SourceSessionIDs: candidate.SourceSessionIDs,
			}),
			Insights:     boundedCandidateInsights(candidate.Insights),
			BundleSHA256: candidate.BundleSHA256,
		}
	}
	evaluations := state.Evaluations
	if len(evaluations) > maxGenerationEvaluations {
		evaluations = evaluations[:maxGenerationEvaluations]
	}
	response.Evaluations = make([]candidateEvaluationResponse, len(evaluations))
	for i, evaluation := range evaluations {
		response.Evaluations[i] = candidateEvaluationResponse{
			ID: evaluation.ID, CandidateID: evaluation.CandidateID,
			Profile: evaluation.Profile, ProfileVersion: evaluation.ProfileVersion,
			EvaluatorVersion: evaluation.EvaluatorVersion, Score: evaluation.Score,
			Decision:             evaluation.Decision,
			CriticalFindingCount: evaluation.CriticalFindingCount,
			WarningFindingCount:  evaluation.WarningFindingCount,
			// Storage admits only canonical arrays within the evaluator's 256 KiB
			// structured-detail limit. Preserve those legal artifacts verbatim;
			// silently replacing a large valid judgment with [] would corrupt history.
			CriterionResults: cloneWireJSON(evaluation.CriterionResults),
			Findings:         cloneWireJSON(evaluation.Findings),
			Strengths:        cloneWireJSON(evaluation.Strengths),
		}
	}
	diagnostics := state.Diagnostics
	if len(diagnostics) > maxGenerationDiagnostics {
		diagnostics = diagnostics[:maxGenerationDiagnostics]
	}
	response.Diagnostics = make([]generationDiagnosticResponse, len(diagnostics))
	for i, diagnostic := range diagnostics {
		response.Diagnostics[i] = generationDiagnosticResponse{
			SessionID: optionalString(diagnostic.SessionID), CandidateID: optionalString(diagnostic.CandidateID),
			Stage:     boundedLifecycleText(diagnostic.Stage, maxLifecycleStageCodePoints),
			Code:      boundedLifecycleText(diagnostic.Code, maxLifecycleCodeCodePoints),
			Message:   boundedLifecycleText(diagnostic.Message, maxLifecycleMessageCodePoints),
			Retryable: diagnostic.Retryable,
			CreatedAt: diagnostic.CreatedAt.UTC().Format(time.RFC3339Nano),
		}
	}
	return response
}

func generationRevisionContentWire(snapshot storage.SkillRevisionSnapshot) generationRevisionContentResponse {
	return generationRevisionContentResponse{
		Name: snapshot.Name, Description: snapshot.Description, Type: snapshot.Type,
		Tags: nonNilWireStrings(snapshot.Tags), Content: snapshot.Content,
		IsAIGenerated:    snapshot.IsAIGenerated,
		SourceSessionIDs: nonNilWireStrings(snapshot.SourceSessionIDs),
	}
}

func effectiveSourceSessionIDs(candidates []storage.GenerationCandidateRecord) []string {
	result := make([]string, 0)
	seen := make(map[string]struct{})
	for _, candidate := range candidates {
		for _, sessionID := range candidate.SourceSessionIDs {
			if _, exists := seen[sessionID]; exists {
				continue
			}
			seen[sessionID] = struct{}{}
			result = append(result, sessionID)
		}
	}
	return result
}

func boundedCandidateInsights(value json.RawMessage) json.RawMessage {
	canonical, err := storage.CanonicalGenerationCandidateInsights(value)
	if err != nil {
		return json.RawMessage(`[]`)
	}
	return canonical
}

func boundedJSON(value, fallback json.RawMessage) json.RawMessage {
	if len(value) == 0 || len(value) > maxLifecycleStructuredJSONBytes || !json.Valid(value) {
		return fallback
	}
	return value
}

func cloneWireJSON(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func boundedLifecycleText(value string, maxCodePoints int) string {
	if !utf8.ValidString(value) {
		value = strings.ToValidUTF8(value, "\uFFFD")
	}
	if utf8.RuneCountInString(value) <= maxCodePoints {
		return value
	}
	return string([]rune(value)[:maxCodePoints])
}

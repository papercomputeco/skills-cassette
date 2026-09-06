package server

import (
	"errors"
	"net/http"
	"time"

	"github.com/papercomputeco/skills-cassette/internal/generation"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

type setLatestRequest struct {
	RevisionID *string `json:"revisionId"`
}

// latestResponse makes the stored explicit pointer distinct from the effective
// newest-public fallback returned after a clear.
type latestResponse struct {
	SkillID                  string            `json:"skillId"`
	ExplicitLatestRevisionID *string           `json:"explicitLatestRevisionId"`
	EffectiveRevision        *revisionResponse `json:"effectiveRevision"`
}

func (s *Server) handleSetLatest(w http.ResponseWriter, r *http.Request) {
	auth, ok := requireAuthContext(w, r)
	if !ok || !s.requireRevisionMetadataStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	var request setLatestRequest
	if err := decodeLifecycleJSONBody(w, r, &request); err != nil || request.RevisionID == nil {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "The request body is invalid.", "")
		return
	}
	revisionID, validRevisionID := requestRevisionID(request.RevisionID)
	if !validRevisionID {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "revisionId must be a UUID.", "")
		return
	}
	record, err := s.revisionMetadataStore.SetExplicitLatestRevision(r.Context(), storage.SetExplicitLatestRevisionInput{
		SkillID: skillID, RevisionID: revisionID, CallerSubject: auth.Subject, ChangedAt: time.Now().UTC(),
	})
	if err != nil {
		s.generationMetrics.ObserveLatestMutation(generation.LatestMutationOperationSet, latestMutationMetricOutcome(err))
		s.writeLifecycleStorageError(w, "set explicit latest revision", err)
		return
	}
	if record == nil {
		s.generationMetrics.ObserveLatestMutation(generation.LatestMutationOperationSet, generation.LatestMutationOutcomeError)
		s.writeLifecycleStorageError(w, "set explicit latest revision", errors.New("metadata store returned no latest projection"))
		return
	}
	s.generationMetrics.ObserveLatestMutation(generation.LatestMutationOperationSet, generation.LatestMutationOutcomeSuccess)
	writeJSON(w, http.StatusOK, latestWire(*record))
}

func (s *Server) handleClearLatest(w http.ResponseWriter, r *http.Request) {
	auth, ok := requireAuthContext(w, r)
	if !ok || !s.requireRevisionMetadataStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	record, err := s.revisionMetadataStore.ClearExplicitLatestRevision(r.Context(), storage.ClearExplicitLatestRevisionInput{
		SkillID: skillID, CallerSubject: auth.Subject, ChangedAt: time.Now().UTC(),
	})
	if err != nil {
		s.generationMetrics.ObserveLatestMutation(generation.LatestMutationOperationClear, latestMutationMetricOutcome(err))
		s.writeLifecycleStorageError(w, "clear explicit latest revision", err)
		return
	}
	if record == nil {
		s.generationMetrics.ObserveLatestMutation(generation.LatestMutationOperationClear, generation.LatestMutationOutcomeError)
		s.writeLifecycleStorageError(w, "clear explicit latest revision", errors.New("metadata store returned no latest projection"))
		return
	}
	s.generationMetrics.ObserveLatestMutation(generation.LatestMutationOperationClear, generation.LatestMutationOutcomeSuccess)
	writeJSON(w, http.StatusOK, latestWire(*record))
}

func latestMutationMetricOutcome(err error) generation.LatestMutationOutcome {
	switch {
	case errors.Is(err, storage.ErrRevisionNotFound), errors.Is(err, storage.ErrSkillNotFound):
		return generation.LatestMutationOutcomeRevisionNotFound
	case errors.Is(err, storage.ErrRevisionNotPublic):
		return generation.LatestMutationOutcomeRevisionNotPublic
	default:
		return generation.LatestMutationOutcomeError
	}
}

func latestWire(record storage.SkillLatestRecord) latestResponse {
	response := latestResponse{
		SkillID:                  record.SkillID,
		ExplicitLatestRevisionID: optionalString(record.ExplicitLatestRevisionID),
	}
	if record.EffectiveRevision != nil {
		value := revisionWire(*record.EffectiveRevision)
		response.EffectiveRevision = &value
	}
	return response
}

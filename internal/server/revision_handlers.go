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

	"github.com/google/uuid"

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const (
	maxRevisionListLimit            = 100
	maxRevisionChangeNoteCodePoints = 1024
	maxRevisionTags                 = storage.MaxRevisionTags
	maxRevisionSources              = storage.MaxRevisionSourceSessionIDs
	maxRevisionContentCodePoints    = storage.MaxRevisionContentCodePoints
)

// revisionSnapshotRequest is one complete immutable content bundle. Slug is
// stable skill identity metadata and therefore is not accepted here.
type revisionSnapshotRequest struct {
	Name             *string   `json:"name"`
	Description      *string   `json:"description"`
	Type             *string   `json:"type"`
	Tags             *[]string `json:"tags"`
	Content          *string   `json:"content"`
	IsAIGenerated    *bool     `json:"isAiGenerated"`
	SourceSessionIDs *[]string `json:"sourceSessionIds"`
}

type appendRevisionRequest struct {
	BasedOnRevisionID *string                 `json:"basedOnRevisionId"`
	SourceRevisionID  *string                 `json:"sourceRevisionId"`
	Snapshot          revisionSnapshotRequest `json:"snapshot"`
	ChangeNote        *string                 `json:"changeNote"`
	IdempotencyKey    *string                 `json:"idempotencyKey"`
}

type setRevisionVisibilityRequest struct {
	IsPublic *bool `json:"isPublic"`
}

// revisionResponse is bounded camelCase immutable content plus separately
// mutable visibility/latest metadata. Sequence and version are presentation and
// ordering fields; all relationships remain revision UUIDs.
type revisionResponse struct {
	ID                string                     `json:"id"`
	SkillID           string                     `json:"skillId"`
	SequenceNumber    int                        `json:"sequenceNumber"`
	Version           string                     `json:"version"`
	BasedOnRevisionID *string                    `json:"basedOnRevisionId"`
	SourceRevisionID  *string                    `json:"sourceRevisionId"`
	Origin            storage.RevisionOrigin     `json:"origin"`
	Snapshot          revisionSnapshotResponse   `json:"snapshot"`
	ContentSHA256     string                     `json:"contentSha256"`
	ChangeNote        *string                    `json:"changeNote"`
	GenerationID      *string                    `json:"generationId"`
	Visibility        revisionVisibilityResponse `json:"visibility"`
	IsExplicitLatest  bool                       `json:"isExplicitLatest"`
	CreatedAt         string                     `json:"createdAt"`
}

type revisionSnapshotResponse struct {
	Name             string   `json:"name"`
	Description      string   `json:"description"`
	Type             string   `json:"type"`
	Tags             []string `json:"tags"`
	Content          string   `json:"content"`
	IsAIGenerated    bool     `json:"isAiGenerated"`
	SourceSessionIDs []string `json:"sourceSessionIds"`
}

type revisionVisibilityResponse struct {
	IsPublic  bool   `json:"isPublic"`
	ChangedAt string `json:"changedAt"`
}

// revisionVisibilityMutationResponse intentionally contains no immutable
// revision content. A public-to-private transition may make that content
// inaccessible to its actor at the mutation boundary, while these bounded
// same-revision metadata fields remain safe for an exact idempotent retry.
type revisionVisibilityMutationResponse struct {
	RevisionID string `json:"revisionId"`
	IsPublic   bool   `json:"isPublic"`
	ChangedAt  string `json:"changedAt"`
}

type revisionsListResponse struct {
	Items      []revisionResponse `json:"items"`
	NextCursor string             `json:"nextCursor,omitempty"`
}

type revisionCursor struct {
	SequenceNumber int    `json:"sequenceNumber"`
	RevisionID     string `json:"revisionId"`
}

func encodeRevisionCursor(cursor revisionCursor) string {
	value, err := json.Marshal(cursor)
	if err != nil {
		panic(fmt.Sprintf("encoding revision cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(value)
}

func decodeRevisionCursor(value string) (revisionCursor, error) {
	if value == "" || len(value) > maxSkillCursorCodePoints {
		return revisionCursor{}, errors.New("invalid revision cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return revisionCursor{}, fmt.Errorf("invalid revision cursor: %w", err)
	}
	var cursor revisionCursor
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cursor); err != nil {
		return revisionCursor{}, fmt.Errorf("invalid revision cursor: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return revisionCursor{}, errors.New("invalid revision cursor")
	}
	if cursor.SequenceNumber < 1 {
		return revisionCursor{}, errors.New("invalid revision cursor")
	}
	canonicalID, err := canonicalUUID(cursor.RevisionID)
	if err != nil {
		return revisionCursor{}, errors.New("invalid revision cursor")
	}
	cursor.RevisionID = canonicalID
	return cursor, nil
}

func (s *Server) handleAppendRevision(w http.ResponseWriter, r *http.Request) {
	auth, ok := requireAuthContext(w, r)
	if !ok || !s.requireRevisionStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	var request appendRevisionRequest
	if err = decodeLifecycleJSONBody(w, r, &request); err != nil {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "The request body is invalid.", "")
		return
	}
	snapshot, err := request.Snapshot.snapshot()
	if err != nil {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, validationMessage(err), "")
		return
	}
	basedOnRevisionID, ok := requestRevisionID(request.BasedOnRevisionID)
	if !ok {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "basedOnRevisionId must be a UUID or null.", "")
		return
	}
	sourceRevisionID, ok := requestRevisionID(request.SourceRevisionID)
	if !ok {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "sourceRevisionId must be a UUID or null.", "")
		return
	}
	if request.IdempotencyKey == nil {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "idempotencyKey is required.", "")
		return
	}
	idempotencyKey := strings.TrimSpace(*request.IdempotencyKey)
	if idempotencyKey == "" ||
		!validBoundedIdentityText(*request.IdempotencyKey, maxLifecycleIdentityCodePoints) ||
		!validBoundedIdentityText(idempotencyKey, maxLifecycleIdentityCodePoints) {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "idempotencyKey is invalid or too long.", "")
		return
	}
	changeNote := ""
	if request.ChangeNote != nil {
		changeNote = strings.TrimSpace(*request.ChangeNote)
		if !validBoundedText(*request.ChangeNote, maxRevisionChangeNoteCodePoints) ||
			!validBoundedText(changeNote, maxRevisionChangeNoteCodePoints) {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "changeNote is invalid or too long.", "")
			return
		}
	}
	origin := storage.RevisionOriginManual
	if sourceRevisionID != "" {
		origin = storage.RevisionOriginDuplicate
	}
	created, err := s.revisionStore.AppendRevision(r.Context(), storage.AppendRevisionInput{
		ID: uuid.NewString(), SkillID: skillID, CreatorSubject: auth.Subject,
		BasedOnRevisionID: basedOnRevisionID, SourceRevisionID: sourceRevisionID,
		Origin: origin, Snapshot: snapshot, ChangeNote: changeNote,
		IdempotencyKey: idempotencyKey, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "append revision", err)
		return
	}
	if created == nil {
		s.writeLifecycleStorageError(w, "append revision", errors.New("revision store returned no revision"))
		return
	}
	accessible, err := s.revisionStore.GetRevision(r.Context(), storage.RevisionReadOpts{
		SkillID: skillID, RevisionID: created.ID, CallerSubject: auth.Subject,
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "read appended revision", err)
		return
	}
	if accessible == nil {
		s.writeLifecycleStorageError(w, "read appended revision", errors.New("revision store returned no appended revision"))
		return
	}
	writeJSON(w, http.StatusCreated, revisionWire(*accessible))
}

func (s *Server) handleListRevisions(w http.ResponseWriter, r *http.Request) {
	if !s.requireRevisionStore(w) || !s.requireSkillReader(w) {
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
	limit := storage.DefaultRevisionListLimit
	if values, present := query["limit"]; present {
		parsed, err := strconv.Atoi(strings.TrimSpace(values[0]))
		if err != nil || parsed < 1 || parsed > maxRevisionListLimit {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "limit must be an integer from 1 through 100.", "")
			return
		}
		limit = parsed
	}
	opts := storage.RevisionListOpts{
		SkillID: skillID, CallerSubject: authContextFromRequest(r).Subject, Limit: limit + 1,
	}
	if values, present := query["cursor"]; present {
		cursor, err := decodeRevisionCursor(strings.TrimSpace(values[0]))
		if err != nil {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "The revision cursor is invalid.", "")
			return
		}
		opts.CursorSequenceNumber = &cursor.SequenceNumber
		opts.CursorRevisionID = cursor.RevisionID
	}
	projection, err := s.skillReader.GetEffectiveSkill(r.Context(), storage.EffectiveSkillReadOpts{
		SkillID: skillID, CallerSubject: opts.CallerSubject,
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "authorize revision history", err)
		return
	}
	if projection == nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	records, err := s.revisionStore.ListRevisions(r.Context(), opts)
	if err != nil {
		s.writeLifecycleStorageError(w, "list revisions", err)
		return
	}
	var nextCursor string
	if len(records) > limit {
		records = records[:limit]
		last := records[len(records)-1].Revision
		nextCursor = encodeRevisionCursor(revisionCursor{
			SequenceNumber: last.SequenceNumber, RevisionID: last.ID,
		})
	}
	items := make([]revisionResponse, len(records))
	for index, record := range records {
		items[index] = revisionWire(record)
	}
	writeJSON(w, http.StatusOK, revisionsListResponse{Items: items, NextCursor: nextCursor})
}

func (s *Server) handleGetRevision(w http.ResponseWriter, r *http.Request) {
	if !s.requireRevisionStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeRevisionNotFound, "The requested revision was not found.", "")
		return
	}
	revisionID, err := canonicalUUID(r.PathValue("revisionId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeRevisionNotFound, "The requested revision was not found.", "")
		return
	}
	record, err := s.revisionStore.GetRevision(r.Context(), storage.RevisionReadOpts{
		SkillID: skillID, RevisionID: revisionID, CallerSubject: authContextFromRequest(r).Subject,
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "get revision", err)
		return
	}
	if record == nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeRevisionNotFound, "The requested revision was not found.", "")
		return
	}
	writeJSON(w, http.StatusOK, revisionWire(*record))
}

func (s *Server) handleSetRevisionVisibility(w http.ResponseWriter, r *http.Request) {
	auth, ok := requireAuthContext(w, r)
	if !ok || !s.requireRevisionMetadataStore(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeRevisionNotFound, "The requested revision was not found.", "")
		return
	}
	revisionID, err := canonicalUUID(r.PathValue("revisionId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeRevisionNotFound, "The requested revision was not found.", "")
		return
	}
	var request setRevisionVisibilityRequest
	if err := decodeLifecycleJSONBody(w, r, &request); err != nil || request.IsPublic == nil {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "The request body is invalid.", "")
		return
	}
	visibility, err := s.revisionMetadataStore.SetRevisionVisibility(r.Context(), storage.SetRevisionVisibilityInput{
		SkillID: skillID, RevisionID: revisionID, CallerSubject: auth.Subject,
		IsPublic: *request.IsPublic, ChangedAt: time.Now().UTC(),
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "set revision visibility", err)
		return
	}
	if visibility == nil || visibility.RevisionID != revisionID {
		s.writeLifecycleStorageError(w, "set revision visibility", errors.New("metadata store returned invalid visibility"))
		return
	}
	writeJSON(w, http.StatusOK, revisionVisibilityMutationWire(*visibility))
}

func (request revisionSnapshotRequest) snapshot() (storage.SkillRevisionSnapshot, error) {
	if request.Name == nil || request.Description == nil || request.Type == nil ||
		request.Tags == nil || request.Content == nil || request.IsAIGenerated == nil ||
		request.SourceSessionIDs == nil {
		return storage.SkillRevisionSnapshot{}, errors.New("a complete revision snapshot is required")
	}
	name := strings.TrimSpace(*request.Name)
	description := strings.TrimSpace(*request.Description)
	skillType := strings.TrimSpace(*request.Type)
	if name == "" || !validBoundedIdentityText(*request.Name, maxLifecycleMessageCodePoints) ||
		!validBoundedIdentityText(name, maxLifecycleMessageCodePoints) {
		return storage.SkillRevisionSnapshot{}, errors.New("the revision name is required and must be at most 1024 Unicode code points")
	}
	if !validBoundedText(*request.Description, maxRevisionContentCodePoints) ||
		!validBoundedText(description, maxRevisionContentCodePoints) {
		return storage.SkillRevisionSnapshot{}, errors.New("the revision description is invalid or too long")
	}
	if !validBoundedIdentityText(*request.Type, maxLifecycleIdentityCodePoints) ||
		!validBoundedIdentityText(skillType, maxLifecycleIdentityCodePoints) ||
		!skill.ValidSkillType(skillType) {
		return storage.SkillRevisionSnapshot{}, errors.New("the revision skill type is invalid")
	}
	if !validBoundedText(*request.Content, maxRevisionContentCodePoints) {
		return storage.SkillRevisionSnapshot{}, errors.New("the revision content is invalid or too long")
	}
	tags, err := boundedUniqueStrings(*request.Tags, maxRevisionTags, "tags")
	if err != nil {
		return storage.SkillRevisionSnapshot{}, err
	}
	sources, message := validateSelectedSessionIDs(*request.SourceSessionIDs)
	if message != "" || len(sources) > maxRevisionSources {
		return storage.SkillRevisionSnapshot{}, errors.New("sourceSessionIds must contain at most 100 unique bounded values")
	}
	return storage.SkillRevisionSnapshot{
		Name: name, Description: description, Type: skillType, Tags: tags,
		Content: *request.Content, IsAIGenerated: *request.IsAIGenerated,
		SourceSessionIDs: sources,
	}, nil
}

func requestRevisionID(value *string) (string, bool) {
	if value == nil {
		return "", true
	}
	id := strings.TrimSpace(*value)
	if id == "" || !validBoundedIdentityText(*value, maxUUIDRepresentationCodePoints) ||
		!validBoundedIdentityText(id, maxUUIDRepresentationCodePoints) {
		return "", false
	}
	canonicalID, err := canonicalUUID(id)
	return canonicalID, err == nil
}

func boundedUniqueStrings(values []string, maximum int, field string) ([]string, error) {
	if len(values) > maximum {
		return nil, fmt.Errorf("%s contains too many values", field)
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || !validBoundedIdentityText(raw, maxLifecycleIdentityCodePoints) ||
			!validBoundedIdentityText(value, maxLifecycleIdentityCodePoints) {
			return nil, fmt.Errorf("%s contains an empty, invalid, or oversized value", field)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%s must not contain duplicate values", field)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func revisionWire(record storage.AccessibleRevisionRecord) revisionResponse {
	revision := record.Revision
	return revisionResponse{
		ID: revision.ID, SkillID: revision.SkillID,
		SequenceNumber: revision.SequenceNumber, Version: revision.Version,
		BasedOnRevisionID: optionalString(revision.BasedOnRevisionID),
		SourceRevisionID:  optionalString(revision.SourceRevisionID),
		Origin:            revision.Origin,
		Snapshot: revisionSnapshotResponse{
			Name: revision.Snapshot.Name, Description: revision.Snapshot.Description,
			Type: revision.Snapshot.Type, Tags: nonNilWireStrings(revision.Snapshot.Tags),
			Content: revision.Snapshot.Content, IsAIGenerated: revision.Snapshot.IsAIGenerated,
			SourceSessionIDs: nonNilWireStrings(revision.Snapshot.SourceSessionIDs),
		},
		ContentSHA256: revision.ContentSHA256, ChangeNote: optionalString(revision.ChangeNote),
		GenerationID:     optionalString(revision.GenerationID),
		Visibility:       revisionVisibilityWire(record.Visibility),
		IsExplicitLatest: record.IsExplicitLatest,
		CreatedAt:        revision.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func revisionVisibilityWire(record storage.RevisionVisibilityRecord) revisionVisibilityResponse {
	return revisionVisibilityResponse{
		IsPublic: record.IsPublic, ChangedAt: record.ChangedAt.UTC().Format(time.RFC3339Nano),
	}
}

func revisionVisibilityMutationWire(record storage.RevisionVisibilityRecord) revisionVisibilityMutationResponse {
	return revisionVisibilityMutationResponse{
		RevisionID: record.RevisionID, IsPublic: record.IsPublic,
		ChangedAt: record.ChangedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) requireRevisionStore(w http.ResponseWriter) bool {
	if s.revisionStore != nil {
		return true
	}
	writeLifecycleError(w, http.StatusNotImplemented, errorCodePersistenceNotConfigured, "Revision persistence is not configured.", "")
	return false
}

func (s *Server) requireRevisionMetadataStore(w http.ResponseWriter) bool {
	if s.revisionMetadataStore != nil {
		return true
	}
	writeLifecycleError(w, http.StatusNotImplemented, errorCodePersistenceNotConfigured, "Revision metadata persistence is not configured.", "")
	return false
}

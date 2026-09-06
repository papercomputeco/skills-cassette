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

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const (
	defaultSkillsLimit       = 24
	maxSkillsLimit           = 100
	maxSkillSlugCodePoints   = 128
	maxSkillQueryCodePoints  = 1024
	maxSkillCursorCodePoints = 1024
	maxExternalFilterValues  = 100
	// The worst accepted rendering is below 11 MiB: at most 6 MiB for the
	// JSON-escaped description, 4 MiB for raw content, and 256 KiB for all
	// other bounded frontmatter plus framing. Twelve MiB leaves fixed headroom
	// without making the response unbounded.
	maxSkillMarkdownOutputCodePoints = 12 << 20
	maxSkillMarkdownOutputBytes      = 12 << 20
)

var errListAuthenticationRequired = errors.New("authenticated list scope required")

// resolveSkillRequest resolves a normalized tenant-local slug. Creator
// attribution comes only from authContext.
type resolveSkillRequest struct {
	Slug *string `json:"slug"`
}

// skillIdentityResponse contains stable identity metadata only. Resolving a
// slug never returns another creator's private revision metadata.
type skillIdentityResponse struct {
	ID                       string  `json:"id"`
	Slug                     string  `json:"slug"`
	ExplicitLatestRevisionID *string `json:"explicitLatestRevisionId"`
	CreatedAt                string  `json:"createdAt"`
}

// effectiveSkillResponse is the bounded viewer-aware card/detail projection.
// EffectiveRevision is public; NewestPrivateRevision can only belong to the
// current viewer; CardRevision selects one of those two without another read.
type effectiveSkillResponse struct {
	ID                       string            `json:"id"`
	Slug                     string            `json:"slug"`
	ExplicitLatestRevisionID *string           `json:"explicitLatestRevisionId"`
	EffectiveRevision        *revisionResponse `json:"effectiveRevision"`
	NewestPrivateRevision    *revisionResponse `json:"newestPrivateRevision"`
	CardRevision             *revisionResponse `json:"cardRevision"`
	HasNewerPrivateRevision  bool              `json:"hasNewerPrivateRevision"`
	DownloadCount            int64             `json:"downloadCount"`
	CreatedAt                string            `json:"createdAt"`
	UpdatedAt                string            `json:"updatedAt"`
}

type effectiveSkillsListResponse struct {
	Items      []effectiveSkillResponse `json:"items"`
	NextCursor string                   `json:"nextCursor,omitempty"`
	Counts     skillCountsResponse      `json:"counts"`
}

type skillCountsResponse struct {
	All  int64 `json:"all"`
	Mine int64 `json:"mine"`
	Team int64 `json:"team"`
}

type sessionSkillsResponse struct {
	Items []effectiveSkillResponse `json:"items"`
}

// skillsCursor is the opaque keyset cursor for effective skill lists. UpdatedAt
// is the selected card revision creation time; downloads is the alternate sort
// key; ID is the stable tiebreak.
//
// The JSON shape {ts, dc, id} is a wire contract shared across a rolling
// deployment: decoders reject unknown keys, so a new key would break paging
// between replicas of different releases in both directions. A keyset boundary
// is only meaningful under the sort it was cut from, so the producing sort is
// recorded inside that shape instead: a recent-order cursor omits dc, and a
// downloads-order cursor carries the sentinel downloadsCursorTimestamp as ts.
// Both stay decodable by earlier releases, which only require ts to be non-zero
// and dc to be non-negative. A cursor carrying a real ts and a dc is a legacy
// cursor from a replica that did not record its sort; it is accepted under
// whichever sort is requested, exactly as every cursor was before binding.
type skillsCursor struct {
	UpdatedAt time.Time `json:"ts"`
	Downloads *int64    `json:"dc,omitempty"`
	ID        string    `json:"id"`
}

const skillSortRecent = "recent"

// downloadsCursorTimestamp marks a downloads-order cursor. It is non-zero so
// earlier decoders accept it, and no revision is created at the Unix epoch, so
// it can never collide with a real recent-order boundary.
var downloadsCursorTimestamp = time.Unix(0, 0).UTC()

// skillsCursorSort names the sort of a list request for its cursors.
func skillsCursorSort(sort string) string {
	if sort == storage.SkillSortDownloads {
		return storage.SkillSortDownloads
	}
	return skillSortRecent
}

// newSkillsCursor cuts a boundary for the given sort in the shape described on
// skillsCursor.
func newSkillsCursor(sort string, updatedAt time.Time, downloads int64, id string) skillsCursor {
	if skillsCursorSort(sort) == storage.SkillSortDownloads {
		return skillsCursor{UpdatedAt: downloadsCursorTimestamp, Downloads: &downloads, ID: id}
	}
	return skillsCursor{UpdatedAt: updatedAt, ID: id}
}

// boundSort reports the sort a cursor was cut under, or "" for a legacy cursor
// that recorded none.
func (cursor skillsCursor) boundSort() string {
	switch {
	case cursor.Downloads == nil:
		return skillSortRecent
	case cursor.UpdatedAt.Equal(downloadsCursorTimestamp):
		return storage.SkillSortDownloads
	default:
		return ""
	}
}

func encodeSkillsCursor(cursor skillsCursor) string {
	value, err := json.Marshal(cursor)
	if err != nil {
		panic(fmt.Sprintf("encoding skills cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(value)
}

func decodeSkillsCursor(token string) (skillsCursor, error) {
	if token == "" || len(token) > maxSkillCursorCodePoints {
		return skillsCursor{}, errors.New("invalid cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return skillsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var cursor skillsCursor
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cursor); err != nil {
		return skillsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return skillsCursor{}, errors.New("invalid cursor")
	}
	canonicalID, err := canonicalUUID(cursor.ID)
	if err != nil || cursor.UpdatedAt.IsZero() || (cursor.Downloads != nil && *cursor.Downloads < 0) {
		return skillsCursor{}, errors.New("invalid cursor")
	}
	cursor.ID = canonicalID
	return cursor, nil
}

// handleResolveSkill creates or resolves a hidden-capable stable identity by
// normalized slug.
func (s *Server) handleResolveSkill(w http.ResponseWriter, r *http.Request) {
	auth, ok := requireAuthContext(w, r)
	if !ok || !s.requireSkillIdentityStore(w) {
		return
	}
	var request resolveSkillRequest
	if err := decodeLifecycleJSONBody(w, r, &request); err != nil || request.Slug == nil {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "The request body is invalid.", "")
		return
	}
	slug := strings.ToLower(strings.TrimSpace(*request.Slug))
	if slug == "" || !validBoundedIdentityText(*request.Slug, maxSkillSlugCodePoints) ||
		!validBoundedIdentityText(slug, maxSkillSlugCodePoints) {
		writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "slug must be a non-empty string of at most 128 Unicode code points.", "")
		return
	}
	record, err := s.skillIdentityStore.ResolveSkill(r.Context(), storage.ResolveSkillInput{
		ID: uuid.NewString(), Slug: slug, CreatorSubject: auth.Subject, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "resolve skill identity", err)
		return
	}
	if record == nil {
		s.writeLifecycleStorageError(w, "resolve skill identity", errors.New("identity store returned no skill"))
		return
	}
	writeJSON(w, http.StatusOK, skillIdentityWire(*record))
}

// handleGetSkill returns one viewer-aware effective projection.
func (s *Server) handleGetSkill(w http.ResponseWriter, r *http.Request) {
	if !s.requireSkillReader(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	record, err := s.skillReader.GetEffectiveSkill(r.Context(), storage.EffectiveSkillReadOpts{
		SkillID: skillID, CallerSubject: authContextFromRequest(r).Subject,
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "get effective skill", err)
		return
	}
	if record == nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	writeJSON(w, http.StatusOK, effectiveSkillWire(*record))
}

// handleListSkills retains keyset pagination, search/scope/download sorting,
// deployment-armed external filters, and the session_id provenance mode while
// sourcing every card from SkillReader.
func (s *Server) handleListSkills(w http.ResponseWriter, r *http.Request) {
	if !s.requireSkillReader(w) {
		return
	}
	sessionValues, sessionPresent := r.URL.Query()["session_id"]
	if sessionPresent {
		if len(sessionValues) != 1 {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "session_id must be supplied at most once.", "")
			return
		}
		rawSessionID := sessionValues[0]
		sessionID := strings.TrimSpace(rawSessionID)
		if !validBoundedIdentityText(rawSessionID, maxLifecycleIdentityCodePoints) ||
			!validBoundedIdentityText(sessionID, maxLifecycleIdentityCodePoints) {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, "session_id is invalid or too long.", "")
			return
		}
		if sessionID != "" {
			s.listSessionSkills(w, r, sessionID)
			return
		}
	}

	auth := authContextFromRequest(r)
	opts, err := s.effectiveSkillListOptions(r, auth)
	if err != nil {
		if errors.Is(err, errListAuthenticationRequired) {
			writeLifecycleError(w, http.StatusUnauthorized, errorCodeUnauthenticated, "Authentication is required.", "")
		} else {
			writeLifecycleError(w, http.StatusBadRequest, errorCodeInvalidRequest, validationMessage(err), "")
		}
		return
	}
	pageLimit := opts.Limit - 1
	records, err := s.skillReader.ListEffectiveSkills(r.Context(), opts)
	if err != nil {
		s.writeEffectiveSkillListError(w, "list effective skills", err)
		return
	}

	var nextCursor string
	if len(records) > pageLimit {
		records = records[:pageLimit]
		last := records[len(records)-1]
		nextCursor = encodeSkillsCursor(newSkillsCursor(
			opts.Sort, last.CardRevision.Revision.CreatedAt, last.Skill.DownloadCount, last.Skill.ID,
		))
	}
	counts, err := s.skillReader.CountEffectiveSkills(r.Context(), storage.EffectiveSkillCountOpts{
		SkillCountOpts: storage.SkillCountOpts{
			Query: opts.Query, Author: auth.Subject, External: opts.External,
		},
		CallerSubject: auth.Subject,
	})
	if err != nil {
		s.writeEffectiveSkillListError(w, "count effective skills", err)
		return
	}
	items := make([]effectiveSkillResponse, len(records))
	for index, record := range records {
		items[index] = effectiveSkillWire(record)
	}
	team := counts.Total - counts.Mine
	if team < 0 {
		team = 0
	}
	writeJSON(w, http.StatusOK, effectiveSkillsListResponse{
		Items: items, NextCursor: nextCursor,
		Counts: skillCountsResponse{All: counts.Total, Mine: counts.Mine, Team: team},
	})
}

func (s *Server) listSessionSkills(w http.ResponseWriter, r *http.Request, sessionID string) {
	records, err := s.skillReader.ListEffectiveSkillsBySession(r.Context(), storage.EffectiveSkillSessionListOpts{
		SessionID: sessionID, CallerSubject: authContextFromRequest(r).Subject, Limit: maxSkillsLimit,
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "list effective skills by session", err)
		return
	}
	if len(records) > maxSkillsLimit {
		records = records[:maxSkillsLimit]
	}
	items := make([]effectiveSkillResponse, len(records))
	for index, record := range records {
		items[index] = effectiveSkillWire(record)
	}
	writeJSON(w, http.StatusOK, sessionSkillsResponse{Items: items})
}

// handleSkillMarkdown resolves only the effective public revision, renders the
// existing SKILL.md distribution format, and keeps download counting best
// effort. It never falls back to viewer-private content.
func (s *Server) handleSkillMarkdown(w http.ResponseWriter, r *http.Request) {
	if !s.requireSkillReader(w) {
		return
	}
	skillID, err := canonicalUUID(r.PathValue("skillId"))
	if err != nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	projection, err := s.skillReader.GetEffectiveSkill(r.Context(), storage.EffectiveSkillReadOpts{
		SkillID: skillID, CallerSubject: authContextFromRequest(r).Subject,
	})
	if err != nil {
		s.writeLifecycleStorageError(w, "resolve skill markdown", err)
		return
	}
	if projection == nil {
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}
	if projection.EffectiveRevision == nil {
		if projection.NewestPrivateRevision != nil {
			writeLifecycleError(w, http.StatusConflict, errorCodeRevisionPrivate, "A private revision cannot be used by this public operation.", "")
			return
		}
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
		return
	}

	revision := projection.EffectiveRevision.Revision
	rendered := skill.RenderSkillMD(&skill.Skill{
		Name: projection.Skill.Slug, Description: revision.Snapshot.Description,
		Version: revision.Version, Tags: nonNilWireStrings(revision.Snapshot.Tags),
		Type: revision.Snapshot.Type, Content: revision.Snapshot.Content,
		Sessions: nonNilWireStrings(revision.Snapshot.SourceSessionIDs), CreatedAt: revision.CreatedAt,
	})
	if len(rendered) > maxSkillMarkdownOutputBytes ||
		utf8.RuneCountInString(rendered) > maxSkillMarkdownOutputCodePoints {
		s.writeLifecycleStorageError(w, "render skill markdown", errors.New("rendered skill markdown exceeds response limit"))
		return
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", projection.Skill.Slug+".md"))
	body := []byte(rendered)
	written, writeErr := w.Write(body)
	if writeErr != nil || written != len(body) {
		s.logger.Warn("write skill markdown", "written_bytes", written, "body_bytes", len(body), "error", writeErr)
		return
	}
	if err = s.distributions.IncrementSkillDownloads(r.Context(), projection.Skill.ID); err != nil {
		s.logger.Warn("increment skill downloads", "error", err)
	}
}

// effectiveSkillListOptions builds storage-level effective list filters. Every
// armed attachment filter is passed to both list and count storage operations.
func (s *Server) effectiveSkillListOptions(r *http.Request, auth authContext) (storage.EffectiveSkillListOpts, error) {
	query := r.URL.Query()
	for _, name := range []string{"limit", "cursor", "q", "scope", "sort"} {
		if len(query[name]) > 1 {
			return storage.EffectiveSkillListOpts{}, fmt.Errorf("%s must be supplied at most once", name)
		}
	}

	limit := defaultSkillsLimit
	if values, present := query["limit"]; present {
		raw := strings.TrimSpace(values[0])
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxSkillsLimit {
			return storage.EffectiveSkillListOpts{}, errors.New("limit must be an integer from 1 through 100")
		}
		limit = parsed
	}
	rawSearch := query.Get("q")
	search := strings.TrimSpace(rawSearch)
	if !validBoundedText(rawSearch, maxSkillQueryCodePoints) ||
		!validBoundedText(search, maxSkillQueryCodePoints) {
		return storage.EffectiveSkillListOpts{}, errors.New("q is invalid or too long")
	}
	opts := storage.EffectiveSkillListOpts{
		SkillListOpts: storage.SkillListOpts{Query: search, Limit: limit + 1},
		CallerSubject: auth.Subject,
	}
	switch query.Get("scope") {
	case "", "all":
	case "mine":
		if auth.Subject == "" {
			return storage.EffectiveSkillListOpts{}, errListAuthenticationRequired
		}
		opts.Author = auth.Subject
	case "team":
		if auth.Subject == "" {
			return storage.EffectiveSkillListOpts{}, errListAuthenticationRequired
		}
		opts.NotAuthor = auth.Subject
	default:
		return storage.EffectiveSkillListOpts{}, errors.New("scope must be all, mine, or team")
	}
	switch query.Get("sort") {
	case "", "recent":
	case storage.SkillSortDownloads:
		opts.Sort = storage.SkillSortDownloads
	default:
		return storage.EffectiveSkillListOpts{}, errors.New("sort must be recent or downloads")
	}
	if values, present := query["cursor"]; present {
		cursor, err := decodeSkillsCursor(strings.TrimSpace(values[0]))
		if err != nil {
			return storage.EffectiveSkillListOpts{}, errors.New("the skills cursor is invalid")
		}
		// A cursor that recorded its sort is only valid under that sort; a
		// legacy cursor recorded none and is accepted under either, as before.
		if bound := cursor.boundSort(); bound != "" && bound != skillsCursorSort(opts.Sort) {
			return storage.EffectiveSkillListOpts{}, errors.New("the skills cursor is invalid")
		}
		opts.CursorID = cursor.ID
		if opts.Sort == storage.SkillSortDownloads {
			downloads := *cursor.Downloads
			opts.CursorDownloads = &downloads
		} else {
			updatedAt := cursor.UpdatedAt
			opts.CursorTs = &updatedAt
		}
	}
	for _, filter := range s.armedFilters() {
		values := query[filter.Param]
		if len(values) == 0 {
			continue
		}
		if len(values) > maxExternalFilterValues {
			return storage.EffectiveSkillListOpts{}, fmt.Errorf("%s has too many values", filter.Param)
		}
		normalized := make([]string, len(values))
		for index, value := range values {
			normalized[index] = NormalizeFilterValue(value, filter.Normalize)
			if !validBoundedIdentityText(value, maxLifecycleIdentityCodePoints) ||
				!validBoundedIdentityText(normalized[index], maxLifecycleIdentityCodePoints) {
				return storage.EffectiveSkillListOpts{}, fmt.Errorf("%s contains an invalid or oversized value", filter.Param)
			}
		}
		opts.External = append(opts.External, storage.ExternalAttachmentFilter{
			View: filter.View, TypeValue: filter.TypeValue, Values: normalized,
		})
	}
	return opts, nil
}

func (s *Server) writeEffectiveSkillListError(w http.ResponseWriter, operation string, err error) {
	if errors.Is(err, storage.ErrExternalViewUnavailable) {
		s.logger.Error(operation, "error", err)
		writeLifecycleError(w, http.StatusServiceUnavailable, "external_view_unavailable", "A configured external filter is temporarily unavailable.", "")
		return
	}
	s.writeLifecycleStorageError(w, operation, err)
}

func effectiveSkillWire(record storage.EffectiveSkillRecord) effectiveSkillResponse {
	response := effectiveSkillResponse{
		ID: record.Skill.ID, Slug: record.Skill.Slug,
		ExplicitLatestRevisionID: optionalString(record.Skill.ExplicitLatestRevisionID),
		HasNewerPrivateRevision:  record.HasNewerPrivateRevision,
		DownloadCount:            record.Skill.DownloadCount,
		CreatedAt:                record.Skill.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:                record.Skill.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if record.EffectiveRevision != nil {
		value := revisionWire(*record.EffectiveRevision)
		response.EffectiveRevision = &value
	}
	if record.NewestPrivateRevision != nil {
		value := revisionWire(*record.NewestPrivateRevision)
		response.NewestPrivateRevision = &value
	}
	if record.CardRevision != nil {
		value := revisionWire(*record.CardRevision)
		response.CardRevision = &value
	}
	return response
}

func skillIdentityWire(record storage.SkillRecord) skillIdentityResponse {
	return skillIdentityResponse{
		ID: record.ID, Slug: record.Slug,
		ExplicitLatestRevisionID: optionalString(record.ExplicitLatestRevisionID),
		CreatedAt:                record.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) requireSkillReader(w http.ResponseWriter) bool {
	if s.skillReader != nil {
		return true
	}
	writeLifecycleError(w, http.StatusNotImplemented, errorCodePersistenceNotConfigured, "Skill revision reads are not configured.", "")
	return false
}

func (s *Server) requireSkillIdentityStore(w http.ResponseWriter) bool {
	if s.skillIdentityStore != nil {
		return true
	}
	writeLifecycleError(w, http.StatusNotImplemented, errorCodePersistenceNotConfigured, "Skill identity persistence is not configured.", "")
	return false
}

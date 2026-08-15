package server

import (
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
	defaultSkillsLimit          = 24
	maxSkillsLimit              = 100
	maxJSONBodyBytes            = 1 << 20
	maxGenerationSessions       = 20
	maxProvenanceSessions       = 100
	maxBriefBytes               = 10_000
	maxFeedbackItems            = 20
	maxFeedbackBytes            = 20_000
	maxRevisionContentBytes     = 200_000
	maxRevisionInstructionBytes = 10_000
)

// errorResponse is the uniform error envelope, matching the shape the Tapes
// skills API served ({"error": "..."}).
type errorResponse struct {
	Error string `json:"error"`
}

// skillsCursor is the opaque keyset cursor for the skills list. It carries the
// last row's id plus both possible sort keys (updated_at and download_count);
// the active sort decides which one the next page filters on. Same base64(JSON)
// encoding sessions use. The console resets the cursor when the sort changes, so
// a cursor is only ever decoded under the sort that produced it.
type skillsCursor struct {
	UpdatedAt time.Time `json:"ts"`
	Downloads int64     `json:"dc"`
	ID        string    `json:"id"`
}

func encodeSkillsCursor(c skillsCursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("encoding skills cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeSkillsCursor(token string) (skillsCursor, error) {
	if token == "" {
		return skillsCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return skillsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c skillsCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return skillsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if c.ID == "" {
		return skillsCursor{}, errors.New("invalid cursor: missing id")
	}
	return c, nil
}

// authSubjectHeader carries the gateway-trusted user id (JWT sub). We trust it
// the same way the core does: the edge gateway stamps it from a validated JWT
// (and strips any client-sent value); in the local clearing the console sets
// it directly since it reaches the cassette without the gateway in path.
const authSubjectHeader = "x-paper-auth-subject"

func authSubjectFromRequest(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(authSubjectHeader))
}

// decodeJSONBody parses the whole request body as exactly one JSON value.
// Unmarshalling the full body (rather than a streaming Decode of the first
// value) refuses trailing data — `{"a":1}{"b":2}` is a malformed request, not
// the first object — matching the pre-cutover fiber BodyParser semantics. An
// empty body returns io.EOF so callers that accept one (publish) can allow it.
func decodeJSONBody(r *http.Request, out any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONBodyBytes+1))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return io.EOF
	}
	if len(body) > maxJSONBodyBytes {
		return errors.New("request body is too large")
	}
	return json.Unmarshal(body, out)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		// Every body written here is a struct of plain fields; failure means
		// the handler is wrong, not the request.
		http.Error(w, `{"error":"encoding response failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// generateSkillRequest describes an ephemeral generation pass. Feedback is
// replayed oldest-first; the endpoint never writes a skill row.
type generateSkillRequest struct {
	SessionIDs []string `json:"sessionIds"`
	Brief      string   `json:"brief"`
	Feedback   []string `json:"feedback"`
}

type reviseSkillRequest struct {
	Content     string `json:"content"`
	Instruction string `json:"instruction"`
}

type generatedSkillResponse struct {
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	Type                  string   `json:"type"`
	Tags                  []string `json:"tags"`
	Content               string   `json:"content"`
	OriginatingSessionIDs []string `json:"originatingSessionIds"`
	IsAIGenerated         bool     `json:"isAiGenerated"`
}

type revisedSkillResponse struct {
	Content string `json:"content"`
}

// skillResponse is the unified Skill shape the console expects (camelCase). id
// is the opaque identity / route key; slug is a cosmetic display label. content
// always lives on the skill row (versions are history only); parentId is null
// unless the skill is a duplicate/fork.
type skillResponse struct {
	ID                    string   `json:"id"`
	Slug                  string   `json:"slug"`
	ParentID              *string  `json:"parentId"`
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	Type                  string   `json:"type"`
	Version               string   `json:"version"`
	Visibility            string   `json:"visibility"`
	Status                string   `json:"status"`
	Tags                  []string `json:"tags"`
	Content               string   `json:"content"`
	IsAIGenerated         bool     `json:"isAiGenerated"`
	OriginatingSessionIDs []string `json:"originatingSessionIds"`
	AuthorID              string   `json:"authorId"`
	DownloadCount         int64    `json:"downloadCount"`
	CreatedAt             string   `json:"createdAt"`
	UpdatedAt             string   `json:"updatedAt"`
}

// skillVersionResponse is one immutable published snapshot.
type skillVersionResponse struct {
	ID            string `json:"id"`
	SkillID       string `json:"skillId"`
	VersionNumber int    `json:"versionNumber"`
	Semver        string `json:"semver"`
	PublishedAt   string `json:"publishedAt"`
	Changelog     string `json:"changelog"`
	Content       string `json:"content"`
	AuthorID      string `json:"authorId"`
}

// skillsListResponse is the paginated list envelope: one keyset page plus the
// opaque next_cursor and the per-tab counts for the active search.
type skillsListResponse struct {
	Items      []skillResponse `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
	Counts     skillCountsResp `json:"counts"`
}

// skillCountsResp are the tab counts for the current search: all matching,
// authored by the caller (mine), and everyone else's (team = all - mine).
type skillCountsResp struct {
	All    int64 `json:"all"`
	Mine   int64 `json:"mine"`
	Team   int64 `json:"team"`
	Drafts int64 `json:"drafts"`
}

// sessionSkillsResponse is the envelope for the skills attributed to one
// session (?session_id=). Unpaginated: a session's skill count is bounded by
// what was generated from it. It matches the shape legacy
// GET /v1/sessions/:id/skills callers received.
type sessionSkillsResponse struct {
	Items []skillResponse `json:"items"`
}

// skillVersionsResponse is the full version history for one skill, newest
// first. TotalCount is the length of Versions — the history is returned whole
// rather than paged, so the two never disagree.
type skillVersionsResponse struct {
	Versions   []skillVersionResponse `json:"versions"`
	TotalCount int                    `json:"totalCount"`
}

// handleGenerateSkill returns an ephemeral draft from source sessions, an
// author brief, or both. The client explicitly creates a skill only after the
// author chooses Save draft or Publish.
func (s *Server) handleGenerateSkill(w http.ResponseWriter, r *http.Request) {
	var req generateSkillRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	sessionIDs, err := normalizeIDs(req.SessionIDs, maxGenerationSessions)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid sessionIds: " + err.Error()})
		return
	}
	feedback, err := normalizeTextItems(req.Feedback, maxFeedbackItems)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid feedback: " + err.Error()})
		return
	}
	brief := strings.TrimSpace(req.Brief)
	if len(brief) > maxBriefBytes || totalBytes(feedback) > maxFeedbackBytes {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "generation instructions are too large"})
		return
	}
	if len(sessionIDs) == 0 && brief == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "at least one sessionId or brief is required"})
		return
	}
	if len(sessionIDs) > 0 && s.querier == nil {
		writeJSON(w, http.StatusNotImplemented, errorResponse{
			Error: "session generation requires a configured core url (CASSETTE_CORE_URL)",
		})
		return
	}

	llmCaller, err := s.skillLLMCaller()
	if err != nil {
		s.writeLLMConfigError(w, err)
		return
	}
	draft, err := skill.NewGenerator(s.querier, llmCaller).Generate(
		r.Context(), sessionIDs, brief, feedback, "workflow", nil,
	)
	if err != nil {
		if errors.Is(err, skill.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "one or more source sessions were not found"})
			return
		}
		if errors.Is(err, skill.ErrNoTurns) {
			writeJSON(w, http.StatusUnprocessableEntity, errorResponse{
				Error: "the source sessions carried nothing the generator could use",
			})
			return
		}
		s.logger.Error("generate skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to generate skill"})
		return
	}

	tags := draft.Tags
	if tags == nil {
		tags = []string{}
	}
	writeJSON(w, http.StatusOK, generatedSkillResponse{
		Name:                  draft.Name,
		Description:           draft.Description,
		Type:                  draft.Type,
		Tags:                  tags,
		Content:               draft.Content,
		OriginatingSessionIDs: sessionIDs,
		IsAIGenerated:         true,
	})
}

func (s *Server) handleReviseSkill(w http.ResponseWriter, r *http.Request) {
	var req reviseSkillRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}
	req.Content = strings.TrimSpace(req.Content)
	req.Instruction = strings.TrimSpace(req.Instruction)
	if req.Content == "" || req.Instruction == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "content and instruction are required"})
		return
	}
	if len(req.Content) > maxRevisionContentBytes || len(req.Instruction) > maxRevisionInstructionBytes {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "revision input is too large"})
		return
	}

	llmCaller, err := s.skillLLMCaller()
	if err != nil {
		s.writeLLMConfigError(w, err)
		return
	}
	content, err := skill.NewGenerator(nil, llmCaller).Revise(r.Context(), req.Content, req.Instruction)
	if err != nil {
		s.logger.Error("revise skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to revise skill"})
		return
	}
	writeJSON(w, http.StatusOK, revisedSkillResponse{Content: content})
}

func (s *Server) skillLLMCaller() (skill.LLMCallFunc, error) {
	cfg := s.llm
	if strings.TrimSpace(cfg.Provider) == "" {
		cfg.Provider = "openai"
	}
	return skill.NewLLMCaller(cfg)
}

func (s *Server) writeLLMConfigError(w http.ResponseWriter, err error) {
	if errors.Is(err, skill.ErrNoAPIKey) {
		writeJSON(w, http.StatusUnprocessableEntity, errorResponse{
			Error: "skill AI operations require a configured LLM provider API key",
		})
		return
	}
	s.logger.Error("configure skill llm", "error", err)
	writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "llm provider not configured"})
}

// handleGetSkill returns a persisted skill by id.
func (s *Server) handleGetSkill(w http.ResponseWriter, r *http.Request) {
	rec, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch skill"})
		return
	}
	if rec == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "skill not found"})
		return
	}
	writeJSON(w, http.StatusOK, skillFromRecord(*rec))
}

// handleListSkills returns one keyset page of skills (newest-edited first)
// plus the per-tab counts for the active search. Query params mirror the
// pre-cutover /v1/skills: limit, cursor (opaque), q (name/description/tag
// search), scope (all|mine|team), sort (downloads).
//
// session_id switches the route to the provenance reverse lookup — the
// skills generated from that session, unpaginated — replacing the legacy
// GET /v1/sessions/:id/skills route, whose path the cassette cannot own.
func (s *Server) handleListSkills(w http.ResponseWriter, r *http.Request) {
	if sessionID := strings.TrimSpace(r.URL.Query().Get("session_id")); sessionID != "" {
		s.listSessionSkills(w, r, sessionID)
		return
	}

	subject := authSubjectFromRequest(r)

	limit := defaultSkillsLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 1 {
			limit = parsed
		}
		if limit > maxSkillsLimit {
			limit = maxSkillsLimit
		}
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	opts := storage.SkillListOpts{Query: query, Limit: limit + 1} // +1 to detect has_more
	switch status := r.URL.Query().Get("status"); status {
	case "":
	case storage.SkillStatusDraft, storage.SkillStatusPublished:
		opts.Status = status
	default:
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "status must be draft or published"})
		return
	}
	if r.URL.Query().Get("sort") == storage.SkillSortDownloads {
		opts.Sort = storage.SkillSortDownloads
	}
	switch r.URL.Query().Get("scope") {
	case "mine":
		opts.Author = subject
	case "team":
		opts.NotAuthor = subject
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		cur, err := decodeSkillsCursor(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
			return
		}
		opts.CursorID = cur.ID
		if opts.Sort == storage.SkillSortDownloads {
			dc := cur.Downloads
			opts.CursorDownloads = &dc
		} else {
			ts := cur.UpdatedAt
			opts.CursorTs = &ts
		}
	}

	recs, err := s.store.ListSkills(r.Context(), opts)
	if err != nil {
		s.logger.Error("list skills", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to list skills"})
		return
	}

	var nextCursor string
	if len(recs) > limit {
		recs = recs[:limit]
		last := recs[len(recs)-1]
		nextCursor = encodeSkillsCursor(skillsCursor{
			UpdatedAt: last.UpdatedAt,
			Downloads: last.DownloadCount,
			ID:        last.ID,
		})
	}

	counts, err := s.store.CountSkills(r.Context(), query, subject)
	if err != nil {
		s.logger.Error("count skills", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to list skills"})
		return
	}

	items := make([]skillResponse, len(recs))
	for i, r := range recs {
		items[i] = skillFromRecord(r)
	}
	writeJSON(w, http.StatusOK, skillsListResponse{
		Items:      items,
		NextCursor: nextCursor,
		Counts: skillCountsResp{
			All:    counts.Total,
			Mine:   counts.Mine,
			Team:   counts.Total - counts.Mine,
			Drafts: counts.Drafts,
		},
	})
}

// listSessionSkills returns the skills generated from a given session
// (reverse lookup over provenance). Small result set, so it's unpaginated —
// the "Skills from this session" panel renders them directly.
func (s *Server) listSessionSkills(w http.ResponseWriter, r *http.Request, sessionID string) {
	recs, err := s.store.ListSkillsBySession(r.Context(), sessionID)
	if err != nil {
		s.logger.Error("list session skills", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to list skills"})
		return
	}
	items := make([]skillResponse, len(recs))
	for i, r := range recs {
		items[i] = skillFromRecord(r)
	}
	writeJSON(w, http.StatusOK, sessionSkillsResponse{Items: items})
}

// updateSkillRequest is the PUT body — all fields optional; only present
// fields are applied onto the existing record.
type updateSkillRequest struct {
	Name                  *string  `json:"name"`
	Description           *string  `json:"description"`
	Type                  *string  `json:"type"`
	Visibility            *string  `json:"visibility"`
	Tags                  []string `json:"tags"`
	Content               *string  `json:"content"`
	OriginatingSessionIDs []string `json:"originatingSessionIds"`
}

// handleUpdateSkill saves edits to a skill's working content/metadata. The
// upsert preserves created_at and author_subject (original creator stays
// authoritative).
func (s *Server) handleUpdateSkill(w http.ResponseWriter, r *http.Request) {
	existing, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get skill for update", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch skill"})
		return
	}
	if existing == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "skill not found"})
		return
	}

	var req updateSkillRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	rec := *existing
	if req.Name != nil {
		rec.Name = *req.Name
		// slug is cosmetic now (the id is identity), so keep it in sync with the
		// name on rename — the SKILL.md filename then tracks the current name.
		if derived := slugifySkillName(rec.Name); derived != "" {
			rec.Slug = derived
		}
	}
	if req.Description != nil {
		rec.Description = *req.Description
	}
	if req.Type != nil {
		if !skill.ValidSkillType(*req.Type) {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid type %q", *req.Type)})
			return
		}
		rec.Type = *req.Type
	}
	if req.Visibility != nil {
		rec.Visibility = *req.Visibility
	}
	if req.Tags != nil {
		rec.Tags = req.Tags
	}
	if req.Content != nil {
		rec.Content = *req.Content
	}
	if req.OriginatingSessionIDs != nil {
		ids, err := normalizeIDs(req.OriginatingSessionIDs, maxProvenanceSessions)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid originatingSessionIds: " + err.Error()})
			return
		}
		rec.GeneratedFromSessionIDs = ids
	}
	rec.UpdatedAt = time.Now().UTC()

	saved, err := s.store.UpsertSkill(r.Context(), rec)
	if err != nil {
		s.logger.Error("update skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to save skill"})
		return
	}
	writeJSON(w, http.StatusOK, skillFromRecord(*saved))
}

// handleDeleteSkill removes a skill and its version history. Owner-gated: only
// the recorded author may delete (unattributed skills are deletable by anyone,
// matching the edit affordance).
func (s *Server) handleDeleteSkill(w http.ResponseWriter, r *http.Request) {
	existing, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get skill for delete", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch skill"})
		return
	}
	if existing == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "skill not found"})
		return
	}

	// Only the creator may delete. An empty author_subject means unattributed
	// (legacy/demo) — deletable by anyone, mirroring the edit gate.
	subject := authSubjectFromRequest(r)
	if existing.AuthorSubject != "" && subject != existing.AuthorSubject {
		writeJSON(w, http.StatusForbidden, errorResponse{Error: "only the creator can delete this skill"})
		return
	}

	if _, err := s.store.DeleteSkill(r.Context(), existing.ID); err != nil {
		s.logger.Error("delete skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to delete skill"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// createSkillRequest persists either an authored or generated draft. Only a
// name is required; lifecycle always starts as draft.
type createSkillRequest struct {
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	Type                  string   `json:"type"`
	Tags                  []string `json:"tags"`
	Content               string   `json:"content"`
	OriginatingSessionIDs []string `json:"originatingSessionIds"`
	IsAIGenerated         bool     `json:"isAiGenerated"`
}

// handleCreateSkill writes a new draft attributed to the caller. Generation
// itself is side-effect-free, so generated metadata and provenance arrive here
// only after the author chooses to save. The server mints the id and slug.
func (s *Server) handleCreateSkill(w http.ResponseWriter, r *http.Request) {
	var req createSkillRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	displayName := strings.TrimSpace(req.Name)
	if displayName == "" {
		displayName = "New skill"
	}
	skillType := strings.TrimSpace(req.Type)
	if skillType == "" {
		skillType = "workflow"
	}
	if !skill.ValidSkillType(skillType) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid type %q", skillType)})
		return
	}

	slug := slugifySkillName(displayName)
	if slug == "" {
		slug = "new-skill"
	}
	originatingSessionIDs, err := normalizeIDs(req.OriginatingSessionIDs, maxProvenanceSessions)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid originatingSessionIds: " + err.Error()})
		return
	}

	now := time.Now().UTC()
	rec := storage.SkillRecord{
		ID:                      uuid.NewString(),
		Slug:                    slug,
		Name:                    displayName,
		Description:             req.Description,
		Type:                    skillType,
		Version:                 "0.1.0",
		Visibility:              "private",
		Status:                  storage.SkillStatusDraft,
		Tags:                    req.Tags,
		Content:                 req.Content,
		IsAIGenerated:           req.IsAIGenerated,
		GeneratedFromSessionIDs: originatingSessionIDs,
		AuthorSubject:           authSubjectFromRequest(r),
		CreatedAt:               now,
		UpdatedAt:               now,
	}
	saved, err := s.store.UpsertSkill(r.Context(), rec)
	if err != nil {
		s.logger.Error("create skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to create skill"})
		return
	}
	writeJSON(w, http.StatusCreated, skillFromRecord(*saved))
}

// publishSkillRequest is the POST versions body.
type publishSkillRequest struct {
	Content   string `json:"content"`
	Changelog string `json:"changelog"`
}

// maxPublishAttempts bounds the retry loop that resolves a concurrent
// version-number collision when two publishes of the same skill race.
const maxPublishAttempts = 4

// handlePublishSkill snapshots the skill's content into an immutable version
// and bumps the skill's current semver (first publish 0.1.0, then patch).
func (s *Server) handlePublishSkill(w http.ResponseWriter, r *http.Request) {
	existing, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get skill for publish", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch skill"})
		return
	}
	if existing == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "skill not found"})
		return
	}
	skillID := existing.ID

	// An empty body is a valid publish — it snapshots the skill's current head.
	// Malformed or truncated JSON is not: rejecting it here keeps a garbled
	// request from minting an unintended version.
	var req publishSkillRequest
	if err := decodeJSONBody(r, &req); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}
	content := req.Content
	if strings.TrimSpace(content) == "" {
		content = existing.Content
	}

	now := time.Now().UTC()

	// Assigning the next version number (a MAX read) and inserting it are two
	// round-trips, so two concurrent publishes of the same skill can pick the
	// same number — the second insert then hits the (skill_id, version_number)
	// unique constraint. Retry on that conflict: the next MAX read sees the
	// committed competitor, so a bounded loop converges instead of 500-ing.
	//
	// The store publishes atomically: the version row and the head bump land
	// together, and the head only advances when this is the highest published
	// number — of two overlapping publishes, the older one can never regress
	// the head the newer one already set.
	var ver *storage.SkillVersionRecord
	for attempt := range maxPublishAttempts {
		n, err := s.store.NextSkillVersionNumber(r.Context(), skillID)
		if err != nil {
			s.logger.Error("next skill version", "error", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to version skill"})
			return
		}
		semver := fmt.Sprintf("0.1.%d", n-1) // n=1 -> 0.1.0, n=2 -> 0.1.1, …

		ver, err = s.store.PublishSkillVersion(r.Context(), storage.SkillVersionRecord{
			SkillID:       skillID,
			VersionNumber: n,
			Semver:        semver,
			Changelog:     req.Changelog,
			Content:       content,
			AuthorSubject: authSubjectFromRequest(r),
			PublishedAt:   now,
		})
		if err == nil {
			break
		}
		if errors.Is(err, storage.ErrSkillVersionConflict) && attempt < maxPublishAttempts-1 {
			continue // a concurrent publish took this number; recompute and retry
		}
		s.logger.Error("publish skill version", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to publish skill"})
		return
	}

	writeJSON(w, http.StatusCreated, skillVersionFromRecord(*ver))
}

// handleListSkillVersions returns a skill's published version history.
func (s *Server) handleListSkillVersions(w http.ResponseWriter, r *http.Request) {
	vers, err := s.store.ListSkillVersions(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("list skill versions", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to list versions"})
		return
	}
	items := make([]skillVersionResponse, len(vers))
	for i, v := range vers {
		items[i] = skillVersionFromRecord(v)
	}
	writeJSON(w, http.StatusOK, skillVersionsResponse{Versions: items, TotalCount: len(items)})
}

// handleDuplicateSkill copies a skill under a fresh id, attributed to the
// duplicating user. Because slug is no longer an identity it can be shared with
// the parent freely — no "-copy" suffix is needed to stay distinct.
func (s *Server) handleDuplicateSkill(w http.ResponseWriter, r *http.Request) {
	existing, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get skill for duplicate", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch skill"})
		return
	}
	if existing == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "skill not found"})
		return
	}

	now := time.Now().UTC()
	rec := *existing
	rec.ID = uuid.NewString()
	rec.Name = existing.Name + " (copy)"
	rec.Slug = existing.Slug // slug is cosmetic; sharing the parent's reads fine
	rec.Visibility = "private"
	rec.Status = storage.SkillStatusDraft
	rec.Version = "0.1.0"
	rec.ParentID = existing.ID
	rec.AuthorSubject = authSubjectFromRequest(r)
	rec.DownloadCount = 0
	rec.CreatedAt = now
	rec.UpdatedAt = now

	saved, err := s.store.UpsertSkill(r.Context(), rec)
	if err != nil {
		s.logger.Error("duplicate skill", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to duplicate skill"})
		return
	}
	writeJSON(w, http.StatusCreated, skillFromRecord(*saved))
}

// handleSkillMarkdown renders a drop-in SKILL.md (frontmatter + body) for the
// "Use this skill" download, via the same renderer the CLI uses.
func (s *Server) handleSkillMarkdown(w http.ResponseWriter, r *http.Request) {
	rec, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get skill for markdown", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch skill"})
		return
	}
	if rec == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "skill not found"})
		return
	}

	// Count the download as a real usage signal (best-effort — never fail the
	// download over a counter write).
	if err := s.store.IncrementSkillDownloads(r.Context(), rec.ID); err != nil {
		s.logger.Warn("increment skill downloads", "error", err)
	}

	// The SKILL.md frontmatter `name` must be the kebab slug (Claude Code
	// matches it to the skill's directory), not the human display name; the
	// display name carries no meaning in the on-disk file.
	sk := &skill.Skill{
		Name:        rec.Slug,
		Description: rec.Description,
		Version:     rec.Version,
		Tags:        rec.Tags,
		Type:        rec.Type,
		Content:     rec.Content,
		Sessions:    rec.GeneratedFromSessionIDs,
		CreatedAt:   rec.CreatedAt,
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", rec.Slug+".md"))
	_, _ = w.Write([]byte(skill.RenderSkillMD(sk)))
}

// skillFromRecord maps the storage row to the camelCase Skill wire shape,
// normalizing nil slices to empty arrays and an empty parent id to JSON null.
func skillFromRecord(rec storage.SkillRecord) skillResponse {
	tags := rec.Tags
	if tags == nil {
		tags = []string{}
	}
	sessions := rec.GeneratedFromSessionIDs
	if sessions == nil {
		sessions = []string{}
	}
	var parent *string
	if rec.ParentID != "" {
		p := rec.ParentID
		parent = &p
	}
	return skillResponse{
		ID:                    rec.ID,
		Slug:                  rec.Slug,
		ParentID:              parent,
		Name:                  rec.Name,
		Description:           rec.Description,
		Type:                  rec.Type,
		Version:               rec.Version,
		Visibility:            rec.Visibility,
		Status:                rec.Status,
		Tags:                  tags,
		Content:               rec.Content,
		IsAIGenerated:         rec.IsAIGenerated,
		OriginatingSessionIDs: sessions,
		AuthorID:              rec.AuthorSubject,
		DownloadCount:         rec.DownloadCount,
		CreatedAt:             rec.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:             rec.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func skillVersionFromRecord(rec storage.SkillVersionRecord) skillVersionResponse {
	return skillVersionResponse{
		ID:            fmt.Sprintf("%s-v%d", rec.SkillID, rec.VersionNumber),
		SkillID:       rec.SkillID,
		VersionNumber: rec.VersionNumber,
		Semver:        rec.Semver,
		PublishedAt:   rec.PublishedAt.UTC().Format(time.RFC3339),
		Changelog:     rec.Changelog,
		Content:       rec.Content,
		AuthorID:      rec.AuthorSubject,
	}
}

func normalizeIDs(values []string, max int) ([]string, error) {
	if len(values) > max {
		return nil, fmt.Errorf("at most %d values are allowed", max)
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, errors.New("values must not be empty")
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func normalizeTextItems(values []string, max int) ([]string, error) {
	if len(values) > max {
		return nil, fmt.Errorf("at most %d values are allowed", max)
	}
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = strings.TrimSpace(value)
		if out[i] == "" {
			return nil, errors.New("values must not be empty")
		}
	}
	return out, nil
}

func totalBytes(values []string) int {
	var total int
	for _, value := range values {
		total += len(value)
	}
	return total
}

// slugifySkillName lowercases and hyphenates an arbitrary name into the
// kebab-case slug the console uses as the URL segment.
func slugifySkillName(name string) string {
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			prevHyphen = false
		case b.Len() > 0 && !prevHyphen:
			b.WriteByte('-')
			prevHyphen = true
		}
	}
	return strings.Trim(b.String(), "-")
}

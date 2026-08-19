package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const (
	defaultSkillsLimit          = storage.DefaultListLimit
	maxSkillsLimit              = 100
	maxGenerationSessions       = 20
	maxBriefBytes               = 10000
	maxRevisionInstructionBytes = 10000
	maxRequestBodyBytes         = 1 << 20
)

type errorResponse struct {
	Error string `json:"error"`
}

type skillsCursor struct {
	UpdatedAt time.Time `json:"updated_at"`
	Downloads int64     `json:"downloads"`
	ID        string    `json:"id"`
}

func encodeSkillsCursor(c skillsCursor) string {
	data, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeSkillsCursor(token string) (skillsCursor, error) {
	var out skillsCursor
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || json.Unmarshal(data, &out) != nil || uuid.Validate(out.ID) != nil {
		return out, errors.New("invalid cursor")
	}
	return out, nil
}

func authSubjectFromRequest(r *http.Request) string { return r.Header.Get("x-paper-auth-subject") }

func decodeJSONBody(r *http.Request, out any) error {
	if r.Body == nil {
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes+1))
	if err := decoder.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type skillResponse struct {
	ID               string    `json:"id"`
	Slug             string    `json:"slug"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	Type             string    `json:"type"`
	Version          string    `json:"version"`
	Visibility       string    `json:"visibility"`
	Tags             []string  `json:"tags"`
	Content          string    `json:"content"`
	IsAIGenerated    bool      `json:"isAiGenerated"`
	SourceSessionIDs []string  `json:"sourceSessionIds"`
	ParentID         *string   `json:"parentId"`
	AuthorID         *string   `json:"authorId"`
	DownloadCount    int64     `json:"downloadCount"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type draftResponse struct {
	SkillID          string    `json:"skillId"`
	Revision         int       `json:"revision"`
	Slug             string    `json:"slug"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	Type             string    `json:"type"`
	Tags             []string  `json:"tags"`
	Content          string    `json:"content"`
	IsAIGenerated    bool      `json:"isAiGenerated"`
	SourceSessionIDs []string  `json:"sourceSessionIds"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type skillVersionResponse struct {
	Version          string    `json:"version"`
	VersionNumber    int       `json:"versionNumber"`
	Slug             string    `json:"slug"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	Type             string    `json:"type"`
	Tags             []string  `json:"tags"`
	Content          string    `json:"content"`
	IsAIGenerated    bool      `json:"isAiGenerated"`
	SourceSessionIDs []string  `json:"sourceSessionIds"`
	Changelog        string    `json:"changelog"`
	AuthorID         *string   `json:"authorId"`
	PublishedAt      time.Time `json:"publishedAt"`
}

type skillsListResponse struct {
	Items      []skillResponse `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
	Counts     skillCountsResp `json:"counts"`
}
type skillCountsResp struct {
	All  int64 `json:"all"`
	Mine int64 `json:"mine"`
	Team int64 `json:"team"`
}
type draftsListResponse struct {
	Items      []draftResponse `json:"items"`
	TotalCount int             `json:"totalCount"`
}
type sessionSkillsResponse struct {
	Items []skillResponse `json:"items"`
}
type skillVersionsResponse struct {
	Versions   []skillVersionResponse `json:"versions"`
	TotalCount int                    `json:"totalCount"`
}

type createDraftRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Type        string   `json:"type"`
	Tags        []string `json:"tags"`
	Content     string   `json:"content"`
}
type generateDraftRequest struct {
	SessionIDs []string `json:"sessionIds"`
	Brief      string   `json:"brief"`
}
type updateDraftRequest struct {
	Revision    int      `json:"revision"`
	Name        *string  `json:"name"`
	Description *string  `json:"description"`
	Type        *string  `json:"type"`
	Tags        []string `json:"tags"`
	Content     *string  `json:"content"`
}
type reviseDraftRequest struct {
	Revision    int    `json:"revision"`
	Instruction string `json:"instruction"`
}
type publishDraftRequest struct {
	Revision  int    `json:"revision"`
	Changelog string `json:"changelog"`
}

func (s *Server) handleCreateDraft(w http.ResponseWriter, r *http.Request) {
	var req createDraftRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, 400, errorResponse{"invalid request body"})
		return
	}
	now := time.Now().UTC()
	identity, draft, err := newDraftRecords(req.Name, req.Description, req.Type, req.Tags, req.Content, authSubjectFromRequest(r), false, nil, "", now)
	if err != nil {
		writeJSON(w, 400, errorResponse{err.Error()})
		return
	}
	saved, err := s.store.CreateDraft(r.Context(), identity, draft)
	if err != nil {
		s.logger.Error("create draft", "error", err)
		writeJSON(w, 500, errorResponse{"failed to create draft"})
		return
	}
	writeJSON(w, http.StatusCreated, draftFromRecord(*saved))
}

func (s *Server) handleGenerateDraft(w http.ResponseWriter, r *http.Request) {
	var req generateDraftRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, 400, errorResponse{"invalid request body"})
		return
	}
	ids, err := normalizeIDs(req.SessionIDs, maxGenerationSessions)
	if err != nil {
		writeJSON(w, 400, errorResponse{"invalid sessionIds: " + err.Error()})
		return
	}
	brief := strings.TrimSpace(req.Brief)
	if len(brief) > maxBriefBytes {
		writeJSON(w, 400, errorResponse{"generation brief is too large"})
		return
	}
	if len(ids) == 0 && brief == "" {
		writeJSON(w, 400, errorResponse{"at least one sessionId or brief is required"})
		return
	}
	if len(ids) > 0 && s.querier == nil {
		writeJSON(w, 501, errorResponse{"session generation requires a configured core url (CASSETTE_CORE_URL)"})
		return
	}
	caller, err := s.skillLLMCaller()
	if err != nil {
		s.writeLLMConfigError(w, err)
		return
	}
	generated, err := skill.NewGenerator(s.querier, caller).Generate(r.Context(), ids, brief, "workflow", nil)
	if err != nil {
		switch {
		case errors.Is(err, skill.ErrNotFound):
			writeJSON(w, 404, errorResponse{"one or more source sessions were not found"})
		case errors.Is(err, skill.ErrNoTurns):
			writeJSON(w, 422, errorResponse{"the source sessions carried nothing the generator could use"})
		case errors.Is(err, skill.ErrGenerationContextTooLarge):
			writeJSON(w, 422, errorResponse{err.Error()})
		default:
			s.logger.Error("generate draft", "error", err)
			writeJSON(w, 500, errorResponse{"failed to generate skill"})
		}
		return
	}
	now := time.Now().UTC()
	identity, draft, err := newDraftRecords(generated.Name, generated.Description, generated.Type, generated.Tags, generated.Content, authSubjectFromRequest(r), true, ids, "", now)
	if err != nil {
		writeJSON(w, 500, errorResponse{"generated an invalid draft"})
		return
	}
	saved, err := s.store.CreateDraft(r.Context(), identity, draft)
	if err != nil {
		s.logger.Error("persist generated draft", "error", err)
		writeJSON(w, 500, errorResponse{"failed to save generated draft"})
		return
	}
	writeJSON(w, http.StatusCreated, draftFromRecord(*saved))
}

func newDraftRecords(name, description, skillType string, tags []string, content, author string, ai bool, sources []string, parent string, now time.Time) (storage.SkillRecord, storage.SkillDraftRecord, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "New skill"
	}
	skillType = strings.TrimSpace(skillType)
	if skillType == "" {
		skillType = "workflow"
	}
	if !skill.ValidSkillType(skillType) {
		return storage.SkillRecord{}, storage.SkillDraftRecord{}, fmt.Errorf("invalid type %q", skillType)
	}
	id := uuid.NewString()
	slug := slugifySkillName(name)
	if slug == "" {
		slug = "new-skill"
	}
	identity := storage.SkillRecord{ID: id, ParentID: parent, AuthorSubject: author, Visibility: "private", CreatedAt: now, UpdatedAt: now}
	draft := storage.SkillDraftRecord{SkillID: id, Slug: slug, Name: name, Description: description, Type: skillType, Tags: nonNil(tags), Content: content, IsAIGenerated: ai, GeneratedFromSessionIDs: nonNil(sources), CreatedAt: now, UpdatedAt: now}
	return identity, draft, nil
}

func (s *Server) handleListDrafts(w http.ResponseWriter, r *http.Request) {
	drafts, err := s.store.ListDrafts(r.Context())
	if err != nil {
		s.logger.Error("list drafts", "error", err)
		writeJSON(w, 500, errorResponse{"failed to list drafts"})
		return
	}
	items := make([]draftResponse, len(drafts))
	for i, d := range drafts {
		items[i] = draftFromRecord(d)
	}
	writeJSON(w, 200, draftsListResponse{Items: items, TotalCount: len(items)})
}
func (s *Server) handleGetDraft(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.GetDraft(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("get draft", "error", err)
		writeJSON(w, 500, errorResponse{"failed to fetch draft"})
		return
	}
	if d == nil {
		writeJSON(w, 404, errorResponse{"draft not found"})
		return
	}
	writeJSON(w, 200, draftFromRecord(*d))
}
func (s *Server) handleInitializeDraft(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.CreateDraftFromPublished(r.Context(), r.PathValue("id"), time.Now().UTC())
	if errors.Is(err, storage.ErrDraftExists) {
		writeJSON(w, 409, errorResponse{"draft already exists"})
		return
	}
	if errors.Is(err, storage.ErrDraftNotFound) {
		writeJSON(w, 404, errorResponse{"published skill not found"})
		return
	}
	if err != nil {
		s.logger.Error("initialize draft", "error", err)
		writeJSON(w, 500, errorResponse{"failed to create draft"})
		return
	}
	writeJSON(w, 201, draftFromRecord(*d))
}

func (s *Server) handleUpdateDraft(w http.ResponseWriter, r *http.Request) {
	var req updateDraftRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, 400, errorResponse{"invalid request body"})
		return
	}
	if req.Revision < 1 {
		writeJSON(w, 400, errorResponse{"revision is required"})
		return
	}
	d, err := s.store.GetDraft(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to fetch draft"})
		return
	}
	if d == nil {
		writeJSON(w, 404, errorResponse{"draft not found"})
		return
	}
	if req.Name != nil {
		d.Name = strings.TrimSpace(*req.Name)
		if d.Name == "" {
			writeJSON(w, 400, errorResponse{"name cannot be empty"})
			return
		}
		d.Slug = slugifySkillName(d.Name)
		if d.Slug == "" {
			d.Slug = "new-skill"
		}
	}
	if req.Description != nil {
		d.Description = *req.Description
	}
	if req.Type != nil {
		if !skill.ValidSkillType(*req.Type) {
			writeJSON(w, 400, errorResponse{"invalid skill type"})
			return
		}
		d.Type = *req.Type
	}
	if req.Tags != nil {
		d.Tags = req.Tags
	}
	if req.Content != nil {
		d.Content = *req.Content
	}
	d.UpdatedAt = time.Now().UTC()
	saved, err := s.store.UpdateDraft(r.Context(), *d, req.Revision)
	if writeDraftStoreError(w, err) {
		return
	}
	writeJSON(w, 200, draftFromRecord(*saved))
}

func (s *Server) handleReviseDraft(w http.ResponseWriter, r *http.Request) {
	var req reviseDraftRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, 400, errorResponse{"invalid request body"})
		return
	}
	req.Instruction = strings.TrimSpace(req.Instruction)
	if req.Revision < 1 || req.Instruction == "" {
		writeJSON(w, 400, errorResponse{"revision and instruction are required"})
		return
	}
	if len(req.Instruction) > maxRevisionInstructionBytes {
		writeJSON(w, 400, errorResponse{"revision instruction is too large"})
		return
	}
	d, err := s.store.GetDraft(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to fetch draft"})
		return
	}
	if d == nil {
		writeJSON(w, 404, errorResponse{"draft not found"})
		return
	}
	if d.Revision != req.Revision {
		writeJSON(w, 409, errorResponse{"draft revision is stale"})
		return
	}
	caller, err := s.skillLLMCaller()
	if err != nil {
		s.writeLLMConfigError(w, err)
		return
	}
	content, err := skill.NewGenerator(nil, caller).Revise(r.Context(), d.Content, req.Instruction)
	if err != nil {
		s.logger.Error("revise draft", "error", err)
		writeJSON(w, 500, errorResponse{"failed to revise draft"})
		return
	}
	d.Content = content
	d.IsAIGenerated = true
	d.UpdatedAt = time.Now().UTC()
	saved, err := s.store.UpdateDraft(r.Context(), *d, req.Revision)
	if writeDraftStoreError(w, err) {
		return
	}
	writeJSON(w, 200, draftFromRecord(*saved))
}

func writeDraftStoreError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, storage.ErrDraftNotFound) {
		writeJSON(w, 404, errorResponse{"draft not found"})
	} else if errors.Is(err, storage.ErrDraftRevisionConflict) {
		writeJSON(w, 409, errorResponse{"draft revision is stale"})
	} else {
		writeJSON(w, 500, errorResponse{"failed to save draft"})
	}
	return true
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
		writeJSON(w, 422, errorResponse{"skill AI operations require a configured LLM provider API key"})
		return
	}
	s.logger.Error("configure skill llm", "error", err)
	writeJSON(w, 500, errorResponse{"llm provider not configured"})
}

func (s *Server) handlePublishDraft(w http.ResponseWriter, r *http.Request) {
	var req publishDraftRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSON(w, 400, errorResponse{"invalid request body"})
		return
	}
	if req.Revision < 1 {
		writeJSON(w, 400, errorResponse{"revision is required"})
		return
	}
	v, err := s.store.PublishDraft(r.Context(), r.PathValue("id"), req.Revision, strings.TrimSpace(req.Changelog), authSubjectFromRequest(r), time.Now().UTC())
	if writeDraftStoreError(w, err) {
		return
	}
	writeJSON(w, 201, versionFromRecord(*v))
}

func (s *Server) handleGetSkill(w http.ResponseWriter, r *http.Request) {
	rec, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to fetch skill"})
		return
	}
	if rec == nil {
		writeJSON(w, 404, errorResponse{"skill not found"})
		return
	}
	writeJSON(w, 200, skillFromRecord(*rec))
}

func (s *Server) handleListSkills(w http.ResponseWriter, r *http.Request) {
	if sessionID := strings.TrimSpace(r.URL.Query().Get("session_id")); sessionID != "" {
		s.listSessionSkills(w, r, sessionID)
		return
	}
	subject := authSubjectFromRequest(r)
	limit := defaultSkillsLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if n, e := strconv.Atoi(raw); e == nil && n >= 1 {
			limit = n
		}
		if limit > maxSkillsLimit {
			limit = maxSkillsLimit
		}
	}
	opts := storage.SkillListOpts{Query: strings.TrimSpace(r.URL.Query().Get("q")), Limit: limit + 1}
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
			writeJSON(w, 400, errorResponse{err.Error()})
			return
		}
		opts.CursorID = cur.ID
		if opts.Sort == storage.SkillSortDownloads {
			opts.CursorDownloads = &cur.Downloads
		} else {
			opts.CursorTs = &cur.UpdatedAt
		}
	}
	recs, err := s.store.ListSkills(r.Context(), opts)
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to list skills"})
		return
	}
	next := ""
	if len(recs) > limit {
		recs = recs[:limit]
		last := recs[len(recs)-1]
		next = encodeSkillsCursor(skillsCursor{UpdatedAt: last.UpdatedAt, Downloads: last.DownloadCount, ID: last.ID})
	}
	counts, err := s.store.CountSkills(r.Context(), opts.Query, subject)
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to list skills"})
		return
	}
	items := make([]skillResponse, len(recs))
	for i, rec := range recs {
		items[i] = skillFromRecord(rec)
	}
	writeJSON(w, 200, skillsListResponse{Items: items, NextCursor: next, Counts: skillCountsResp{All: counts.Total, Mine: counts.Mine, Team: counts.Total - counts.Mine}})
}
func (s *Server) listSessionSkills(w http.ResponseWriter, r *http.Request, id string) {
	recs, err := s.store.ListSkillsBySession(r.Context(), id)
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to list skills"})
		return
	}
	items := make([]skillResponse, len(recs))
	for i, rec := range recs {
		items[i] = skillFromRecord(rec)
	}
	writeJSON(w, 200, sessionSkillsResponse{Items: items})
}

func (s *Server) handleDeleteSkill(w http.ResponseWriter, r *http.Request) {
	identity, err := s.store.GetSkillIdentity(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to fetch skill"})
		return
	}
	if identity == nil {
		writeJSON(w, 404, errorResponse{"skill not found"})
		return
	}
	subject := authSubjectFromRequest(r)
	if identity.AuthorSubject != "" && subject != identity.AuthorSubject {
		writeJSON(w, 403, errorResponse{"only the creator can delete this skill"})
		return
	}
	if _, err := s.store.DeleteSkill(r.Context(), identity.ID); err != nil {
		writeJSON(w, 500, errorResponse{"failed to delete skill"})
		return
	}
	w.WriteHeader(204)
}

func (s *Server) handleListSkillVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := s.store.ListSkillVersions(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to list versions"})
		return
	}
	items := make([]skillVersionResponse, len(versions))
	for i, v := range versions {
		items[i] = versionFromRecord(v)
	}
	writeJSON(w, 200, skillVersionsResponse{Versions: items, TotalCount: len(items)})
}

func (s *Server) handleDuplicateSkill(w http.ResponseWriter, r *http.Request) {
	source, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to fetch skill"})
		return
	}
	if source == nil {
		writeJSON(w, 404, errorResponse{"skill not found"})
		return
	}
	now := time.Now().UTC()
	identity, draft, err := newDraftRecords(source.Name+" (copy)", source.Description, source.Type, source.Tags, source.Content, authSubjectFromRequest(r), false, nil, source.ID, now)
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to duplicate skill"})
		return
	}
	saved, err := s.store.CreateDraft(r.Context(), identity, draft)
	if err != nil {
		writeJSON(w, 500, errorResponse{"failed to duplicate skill"})
		return
	}
	writeJSON(w, 201, draftFromRecord(*saved))
}

func (s *Server) handleSkillMarkdown(w http.ResponseWriter, r *http.Request) {
	rec, err := s.store.GetSkill(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{"failed to fetch skill"})
		return
	}
	if rec == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{"skill not found"})
		return
	}
	if err := s.store.IncrementSkillDownloads(r.Context(), rec.ID); err != nil {
		s.logger.Warn("increment skill downloads", "error", err)
	}
	// On disk, the skill name must match its directory-safe slug.
	sk := &skill.Skill{Name: rec.Slug, Description: rec.Description, Type: rec.Type, Version: rec.Version, Tags: rec.Tags, Content: rec.Content, Sessions: rec.GeneratedFromSessionIDs, CreatedAt: rec.CreatedAt}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", rec.Slug+".md"))
	_, _ = w.Write([]byte(skill.RenderSkillMD(sk)))
}

func skillFromRecord(rec storage.SkillRecord) skillResponse {
	return skillResponse{ID: rec.ID, Slug: rec.Slug, Name: rec.Name, Description: rec.Description, Type: rec.Type, Version: rec.Version, Visibility: rec.Visibility, Tags: nonNil(rec.Tags), Content: rec.Content, IsAIGenerated: rec.IsAIGenerated, SourceSessionIDs: nonNil(rec.GeneratedFromSessionIDs), ParentID: nullable(rec.ParentID), AuthorID: nullable(rec.AuthorSubject), DownloadCount: rec.DownloadCount, CreatedAt: rec.CreatedAt, UpdatedAt: rec.UpdatedAt}
}
func draftFromRecord(d storage.SkillDraftRecord) draftResponse {
	return draftResponse{SkillID: d.SkillID, Revision: d.Revision, Slug: d.Slug, Name: d.Name, Description: d.Description, Type: d.Type, Tags: nonNil(d.Tags), Content: d.Content, IsAIGenerated: d.IsAIGenerated, SourceSessionIDs: nonNil(d.GeneratedFromSessionIDs), CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
}
func versionFromRecord(v storage.SkillVersionRecord) skillVersionResponse {
	return skillVersionResponse{Version: v.Semver, VersionNumber: v.VersionNumber, Slug: v.Slug, Name: v.Name, Description: v.Description, Type: v.Type, Tags: nonNil(v.Tags), Content: v.Content, IsAIGenerated: v.IsAIGenerated, SourceSessionIDs: nonNil(v.GeneratedFromSessionIDs), Changelog: v.Changelog, AuthorID: nullable(v.AuthorSubject), PublishedAt: v.PublishedAt}
}
func nullable(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
func nonNil[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

func normalizeIDs(values []string, max int) ([]string, error) {
	if len(values) > max {
		return nil, fmt.Errorf("at most %d values are allowed", max)
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			return nil, errors.New("values cannot be empty")
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out, nil
}

var nonSlugChars = regexp.MustCompile(`[^a-z0-9]+`)

func slugifySkillName(name string) string {
	return strings.Trim(nonSlugChars.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-"), "-")
}

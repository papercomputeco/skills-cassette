package server_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const authSubjectHeader = "x-paper-auth-subject"

func doJSON(srv *server.Server, method, path, body, author string) (map[string]any, int) {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if author != "" {
		req.Header.Set(authSubjectHeader, author)
	}
	recorder := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recorder, req)
	var out map[string]any
	if recorder.Body.Len() > 0 {
		_ = json.Unmarshal(recorder.Body.Bytes(), &out)
	}
	return out, recorder.Code
}

func newSkillsServer(store storage.Store) *server.Server {
	return server.New(server.Config{}, store, nil, nil)
}

func seedPublished(store *storage.MemoryStore, id string, sources []string, ai bool) {
	seedPublishedAs(store, id, sources, ai, "user-seed")
}

func seedPublishedAs(store *storage.MemoryStore, id string, sources []string, ai bool, author string) {
	now := time.Now().UTC()
	_, err := store.CreateDraft(context.Background(), storage.SkillRecord{
		ID: id, AuthorSubject: author, Visibility: "private", CreatedAt: now, UpdatedAt: now,
	}, storage.SkillDraftRecord{SkillID: id, Slug: id, Name: "Skill " + id, Description: "desc", Type: "workflow",
		Tags: []string{"test"}, Content: "# published", IsAIGenerated: ai, GeneratedFromSessionIDs: sources,
		CreatedAt: now, UpdatedAt: now})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.PublishDraft(context.Background(), id, 1, "first", "publisher", now)
	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("Persisted skill drafts", func() {
	It("creates an authored draft without publishing it", func() {
		store := storage.NewMemoryStore()
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"My Draft","content":"# work"}`, "author")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("revision", float64(1)))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", false))
		Expect(body).To(HaveKeyWithValue("sourceSessionIds", BeEmpty()))
		id := body["skillId"].(string)
		published, err := store.GetSkill(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		Expect(published).To(BeNil())
		list, status := doJSON(srv, http.MethodGet, "/api/skills", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(list["items"]).To(BeEmpty())
	})

	It("lists tenant-wide drafts", func() {
		store := storage.NewMemoryStore()
		srv := newSkillsServer(store)
		_, _ = doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"One"}`, "a")
		_, _ = doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"Two"}`, "b")
		body, status := doJSON(srv, http.MethodGet, "/api/skills/drafts", "", "viewer")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body["items"]).To(HaveLen(2))
		Expect(body).To(HaveKeyWithValue("totalCount", float64(2)))
	})

	It("updates draft fields with optimistic concurrency and preserves server metadata", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		draft, err := store.CreateDraft(context.Background(), storage.SkillRecord{ID: "s", CreatedAt: now, UpdatedAt: now}, storage.SkillDraftRecord{
			SkillID: "s", Slug: "old", Name: "Old", Type: "workflow", Content: "# old", IsAIGenerated: true,
			GeneratedFromSessionIDs: []string{"sess-1"}, CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPut, "/api/skills/s/draft", `{"revision":1,"name":"New","content":"# new","sourceSessionIds":["tamper"],"isAiGenerated":false}`, "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(HaveKeyWithValue("revision", float64(2)))
		Expect(body).To(HaveKeyWithValue("sourceSessionIds", ConsistOf("sess-1")))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", true))
		_, status = doJSON(srv, http.MethodPut, "/api/skills/s/draft", `{"revision":1,"content":"stale"}`, "")
		Expect(status).To(Equal(http.StatusConflict))
		Expect(draft.Revision).To(Equal(1))
	})

	It("initializes a working draft from a publication without exposing later edits", func() {
		store := storage.NewMemoryStore()
		seedPublished(store, "s", nil, false)
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/s/draft", "", "")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("content", "# published"))
		_, status = doJSON(srv, http.MethodPost, "/api/skills/s/draft", "", "")
		Expect(status).To(Equal(http.StatusConflict))
		_, status = doJSON(srv, http.MethodPut, "/api/skills/s/draft", `{"revision":1,"name":"Unpublished name","tags":["pending"],"content":"# unpublished"}`, "")
		Expect(status).To(Equal(http.StatusOK))
		published, status := doJSON(srv, http.MethodGet, "/api/skills/s", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(published).To(HaveKeyWithValue("name", "Skill s"))
		Expect(published).To(HaveKeyWithValue("tags", ConsistOf("test")))
		Expect(published).To(HaveKeyWithValue("content", "# published"))
	})

	It("publishes a complete snapshot and consumes the draft", func() {
		store := storage.NewMemoryStore()
		srv := newSkillsServer(store)
		created, _ := doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"Ready","description":"d","tags":["x"],"content":"# ready"}`, "author")
		id := created["skillId"].(string)
		version, status := doJSON(srv, http.MethodPost, "/api/skills/"+id+"/publish", `{"revision":1,"changelog":"first"}`, "publisher")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(version).To(HaveKeyWithValue("name", "Ready"))
		Expect(version).To(HaveKeyWithValue("version", "0.1.0"))
		_, status = doJSON(srv, http.MethodGet, "/api/skills/"+id+"/draft", "", "")
		Expect(status).To(Equal(http.StatusNotFound))
		published, status := doJSON(srv, http.MethodGet, "/api/skills/"+id, "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(published).To(HaveKeyWithValue("content", "# ready"))
		versions, status := doJSON(srv, http.MethodGet, "/api/skills/"+id+"/versions", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(versions).To(HaveKeyWithValue("totalCount", float64(1)))
	})

	It("rejects stale publication without consuming the draft", func() {
		store := storage.NewMemoryStore()
		srv := newSkillsServer(store)
		created, _ := doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"Draft"}`, "")
		id := created["skillId"].(string)
		_, status := doJSON(srv, http.MethodPost, "/api/skills/"+id+"/publish", `{"revision":2}`, "")
		Expect(status).To(Equal(http.StatusConflict))
		draft, err := store.GetDraft(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).NotTo(BeNil())
	})

	It("duplicates a publication into an authored draft without generation provenance", func() {
		store := storage.NewMemoryStore()
		seedPublished(store, "s", []string{"sess-1"}, true)
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/s/duplicate", "", "duplicator")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", false))
		Expect(body["sourceSessionIds"]).To(BeEmpty())
		identity, err := store.GetSkillIdentity(context.Background(), body["skillId"].(string))
		Expect(err).NotTo(HaveOccurred())
		Expect(identity.ParentID).To(Equal("s"))
	})

	It("keeps creator-gated delete behavior for draft-only identities", func() {
		store := storage.NewMemoryStore()
		srv := newSkillsServer(store)
		created, _ := doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"Draft"}`, "owner")
		id := created["skillId"].(string)
		_, status := doJSON(srv, http.MethodDelete, "/api/skills/"+id, "", "other")
		Expect(status).To(Equal(http.StatusForbidden))
		_, status = doJSON(srv, http.MethodDelete, "/api/skills/"+id, "", "owner")
		Expect(status).To(Equal(http.StatusNoContent))
	})

	It("reverse lookup and markdown read current published snapshots", func() {
		store := storage.NewMemoryStore()
		seedPublished(store, "s", []string{"sess-1"}, true)
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodGet, "/api/skills?session_id=sess-1", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body["items"]).To(HaveLen(1))
		recorder := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/skills/s/skill.md", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Header().Get("Content-Type")).To(Equal("text/markdown; charset=utf-8"))
		Expect(recorder.Header().Get("Content-Disposition")).To(ContainSubstring("s.md"))
		Expect(recorder.Body.String()).To(ContainSubstring(`name: "s"`))
		Expect(recorder.Body.String()).To(ContainSubstring("# published"))
		Expect(recorder.Body.String()).To(ContainSubstring("sess-1"))
		published, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(published.DownloadCount).To(Equal(int64(1)))
	})

	It("paginates published skills with an opaque cursor", func() {
		store := storage.NewMemoryStore()
		for range 25 {
			seedPublished(store, uuid.NewString(), nil, false)
		}
		srv := newSkillsServer(store)
		first, status := doJSON(srv, http.MethodGet, "/api/skills", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(first["items"]).To(HaveLen(24))
		cursor, ok := first["next_cursor"].(string)
		Expect(ok).To(BeTrue())
		second, status := doJSON(srv, http.MethodGet, "/api/skills?cursor="+cursor, "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(second["items"]).To(HaveLen(1))
	})

	It("rejects malformed and forged cursors", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		_, status := doJSON(srv, http.MethodGet, "/api/skills?cursor=not-base64", "", "")
		Expect(status).To(Equal(http.StatusBadRequest))
		payload, err := json.Marshal(map[string]any{"id": "not-a-uuid"})
		Expect(err).NotTo(HaveOccurred())
		forged := base64.RawURLEncoding.EncodeToString(payload)
		_, status = doJSON(srv, http.MethodGet, "/api/skills?cursor="+forged, "", "")
		Expect(status).To(Equal(http.StatusBadRequest))
	})

	It("filters published skills by attribution scope and reports counts", func() {
		store := storage.NewMemoryStore()
		seedPublishedAs(store, uuid.NewString(), nil, false, "me")
		seedPublishedAs(store, uuid.NewString(), nil, false, "other")
		srv := newSkillsServer(store)
		mine, status := doJSON(srv, http.MethodGet, "/api/skills?scope=mine", "", "me")
		Expect(status).To(Equal(http.StatusOK))
		Expect(mine["items"]).To(HaveLen(1))
		Expect(mine["counts"]).To(HaveKeyWithValue("all", float64(2)))
		team, status := doJSON(srv, http.MethodGet, "/api/skills?scope=team", "", "me")
		Expect(status).To(Equal(http.StatusOK))
		Expect(team["items"]).To(HaveLen(1))
	})

	It("sorts published skills by downloads", func() {
		store := storage.NewMemoryStore()
		popular, other := uuid.NewString(), uuid.NewString()
		seedPublished(store, popular, nil, false)
		seedPublished(store, other, nil, false)
		Expect(store.IncrementSkillDownloads(context.Background(), popular)).To(Succeed())
		Expect(store.IncrementSkillDownloads(context.Background(), popular)).To(Succeed())
		body, status := doJSON(newSkillsServer(store), http.MethodGet, "/api/skills?sort=downloads", "", "")
		Expect(status).To(Equal(http.StatusOK))
		items := body["items"].([]any)
		Expect(items[0]).To(HaveKeyWithValue("id", popular))
	})

	It("rejects malformed publish input without consuming the draft", func() {
		store := storage.NewMemoryStore()
		srv := newSkillsServer(store)
		created, _ := doJSON(srv, http.MethodPost, "/api/skills/drafts", `{"name":"Draft"}`, "")
		id := created["skillId"].(string)
		_, status := doJSON(srv, http.MethodPost, "/api/skills/"+id+"/publish", `{`, "")
		Expect(status).To(Equal(http.StatusBadRequest))
		draft, err := store.GetDraft(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).NotTo(BeNil())
	})

	It("returns 404 for missing published skills and deletes", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		_, status := doJSON(srv, http.MethodGet, "/api/skills/missing", "", "")
		Expect(status).To(Equal(http.StatusNotFound))
		_, status = doJSON(srv, http.MethodDelete, "/api/skills/missing", "", "")
		Expect(status).To(Equal(http.StatusNotFound))
	})
})

type fakeQuerier struct {
	summaries map[string][]skill.TraceSummary
	notFound  bool
}

func (q *fakeQuerier) TraceSummaries(_ context.Context, id string) ([]skill.TraceSummary, error) {
	if q.notFound {
		return nil, fmt.Errorf("missing: %w", skill.ErrNotFound)
	}
	return q.summaries[id], nil
}
func (q *fakeQuerier) Trace(_ context.Context, _ string) (*skill.Trace, error) { return nil, nil }

var _ = Describe("Generated persisted drafts", func() {
	stubOpenAI := func(generated map[string]any) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			content, _ := json.Marshal(generated)
			_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": string(content)}}}})
		}))
	}
	newGenerateServer := func(store storage.Store, q skill.Querier, llm string) *server.Server {
		return server.New(server.Config{LLM: skill.LLMCallerConfig{Provider: "openai", APIKey: "test", BaseURL: llm}}, store, q, nil)
	}

	It("persists successful generation with exact server-owned provenance", func() {
		llm := stubOpenAI(map[string]any{"name": "Generated", "description": "d", "tags": []string{"x"}, "content": "# generated"})
		defer llm.Close()
		store := storage.NewMemoryStore()
		q := &fakeQuerier{summaries: map[string][]skill.TraceSummary{"sess-1": {{TraceID: "t", UserPrompt: "do it", ResponsePreview: "done"}}}}
		srv := newGenerateServer(store, q, llm.URL)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", `{"sessionIds":["sess-1","sess-1"],"brief":"focus"}`, "author")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("revision", float64(1)))
		Expect(body).To(HaveKeyWithValue("sourceSessionIds", ConsistOf("sess-1")))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", true))
		drafts, err := store.ListDrafts(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(drafts).To(HaveLen(1))
	})

	It("supports brief-only generation", func() {
		llm := stubOpenAI(map[string]any{"name": "Brief", "content": "# b"})
		defer llm.Close()
		store := storage.NewMemoryStore()
		srv := newGenerateServer(store, nil, llm.URL)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", `{"brief":"release process"}`, "")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body["sourceSessionIds"]).To(BeEmpty())
	})

	It("rejects the complete source set when the transcript budget is exceeded", func() {
		store := storage.NewMemoryStore()
		q := &fakeQuerier{summaries: map[string][]skill.TraceSummary{"huge": {{TraceID: "t", UserPrompt: strings.Repeat("x", 30001), ResponsePreview: "done"}}}}
		srv := newGenerateServer(store, q, "http://unused.invalid")
		body, status := doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", `{"sessionIds":["huge"]}`, "")
		Expect(status).To(Equal(http.StatusUnprocessableEntity))
		Expect(body["error"]).To(ContainSubstring("30000"))
		drafts, err := store.ListDrafts(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(drafts).To(BeEmpty())
	})

	It("revises server-side draft content and conditionally persists it", func() {
		llm := stubOpenAI(map[string]any{"content": "# revised"})
		defer llm.Close()
		store := storage.NewMemoryStore()
		base := newGenerateServer(store, nil, llm.URL)
		created, _ := doJSON(base, http.MethodPost, "/api/skills/drafts", `{"name":"D","content":"# original"}`, "")
		id := created["skillId"].(string)
		body, status := doJSON(base, http.MethodPost, "/api/skills/"+id+"/draft/revise", `{"revision":1,"instruction":"improve"}`, "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(HaveKeyWithValue("content", "# revised"))
		Expect(body).To(HaveKeyWithValue("revision", float64(2)))
		_, status = doJSON(base, http.MethodPost, "/api/skills/"+id+"/draft/revise", `{"revision":1,"instruction":"stale"}`, "")
		Expect(status).To(Equal(http.StatusConflict))
	})

	It("validates generation sources before inference", func() {
		srv := newGenerateServer(storage.NewMemoryStore(), nil, "http://unused.invalid")
		_, status := doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", `{}`, "")
		Expect(status).To(Equal(http.StatusBadRequest))
		ids := make([]string, 21)
		for i := range ids {
			ids[i] = fmt.Sprintf("session-%d", i)
		}
		payload, err := json.Marshal(map[string]any{"sessionIds": ids})
		Expect(err).NotTo(HaveOccurred())
		_, status = doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", string(payload), "")
		Expect(status).To(Equal(http.StatusBadRequest))
	})

	It("maps missing and empty source sessions", func() {
		missing := newGenerateServer(storage.NewMemoryStore(), &fakeQuerier{notFound: true}, "http://unused.invalid")
		_, status := doJSON(missing, http.MethodPost, "/api/skills/drafts/generate", `{"sessionIds":["missing"]}`, "")
		Expect(status).To(Equal(http.StatusNotFound))
		empty := newGenerateServer(storage.NewMemoryStore(), &fakeQuerier{summaries: map[string][]skill.TraceSummary{}}, "http://unused.invalid")
		_, status = doJSON(empty, http.MethodPost, "/api/skills/drafts/generate", `{"sessionIds":["empty"]}`, "")
		Expect(status).To(Equal(http.StatusUnprocessableEntity))
	})

	It("requires core configuration only for session-backed generation", func() {
		srv := server.New(server.Config{LLM: skill.LLMCallerConfig{Provider: "openai", APIKey: "test"}}, storage.NewMemoryStore(), nil, nil)
		_, status := doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", `{"sessionIds":["session"]}`, "")
		Expect(status).To(Equal(http.StatusNotImplemented))
	})

	It("maps a missing provider key to 422", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		_, status := doJSON(srv, http.MethodPost, "/api/skills/drafts/generate", `{"brief":"write it"}`, "")
		Expect(status).To(Equal(http.StatusUnprocessableEntity))
	})
})

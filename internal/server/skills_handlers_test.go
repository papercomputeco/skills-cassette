package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const authSubjectHeader = "x-paper-auth-subject"

// seedSkill seeds a skill whose id equals the given slug so URLs read as
// /api/skills/<slug>. The memory store is id-keyed, not UUID-checked, so
// readable ids keep the specs legible.
func seedSkill(store *storage.MemoryStore, slug string) {
	now := time.Now().UTC()
	_, err := store.UpsertSkill(context.Background(), storage.SkillRecord{
		ID:                      slug,
		Slug:                    slug,
		Name:                    "Debug React Hooks",
		Description:             "desc",
		Type:                    "workflow",
		Version:                 "0.1.0",
		Visibility:              "private",
		Tags:                    []string{"react"},
		Content:                 "# body",
		IsAIGenerated:           true,
		GeneratedFromSessionIDs: []string{"sess-1"},
		AuthorSubject:           "user-seed",
		CreatedAt:               now,
		UpdatedAt:               now,
	})
	Expect(err).NotTo(HaveOccurred())
}

// doJSON issues a request (optionally with an auth-subject header) against the
// handler and returns the decoded body map plus the status code.
func doJSON(srv *server.Server, method, path, body, author string) (map[string]any, int) {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewBufferString(body)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, rdr)
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

var _ = Describe("Skills handlers", func() {
	It("returns a unified camelCase skill", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "debug-react-hooks")
		srv := newSkillsServer(store)

		body, status := doJSON(srv, http.MethodGet, "/api/skills/debug-react-hooks", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(HaveKeyWithValue("id", "debug-react-hooks"))
		Expect(body).To(HaveKeyWithValue("slug", "debug-react-hooks"))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", true))
		Expect(body).To(HaveKey("originatingSessionIds"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-seed"))
		Expect(body).To(HaveKeyWithValue("version", "0.1.0"))
		Expect(body).To(HaveKeyWithValue("parentId", BeNil()))
	})

	It("returns 404 when the skill is absent", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		_, status := doJSON(srv, http.MethodGet, "/api/skills/missing", "", "")
		Expect(status).To(Equal(http.StatusNotFound))
	})

	It("lists skills with counts", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "a-skill")
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodGet, "/api/skills", "", "user-seed")
		Expect(status).To(Equal(http.StatusOK))
		items, _ := body["items"].([]any)
		Expect(items).To(HaveLen(1))
		counts, _ := body["counts"].(map[string]any)
		Expect(counts).To(HaveKeyWithValue("all", float64(1)))
		Expect(counts).To(HaveKeyWithValue("mine", float64(1)))
		Expect(counts).To(HaveKeyWithValue("team", float64(0)))
	})

	It("pages the list with an opaque keyset cursor", func() {
		store := storage.NewMemoryStore()
		for i := range 3 {
			seedSkill(store, fmt.Sprintf("skill-%d", i))
		}
		srv := newSkillsServer(store)

		body, status := doJSON(srv, http.MethodGet, "/api/skills?limit=2", "", "")
		Expect(status).To(Equal(http.StatusOK))
		firstPage, _ := body["items"].([]any)
		Expect(firstPage).To(HaveLen(2))
		cursor, _ := body["next_cursor"].(string)
		Expect(cursor).NotTo(BeEmpty())

		body, status = doJSON(srv, http.MethodGet, "/api/skills?limit=2&cursor="+cursor, "", "")
		Expect(status).To(Equal(http.StatusOK))
		secondPage, _ := body["items"].([]any)
		Expect(secondPage).To(HaveLen(1))
		Expect(body).NotTo(HaveKey("next_cursor"))

		seen := map[string]bool{}
		for _, page := range [][]any{firstPage, secondPage} {
			for _, item := range page {
				id, _ := item.(map[string]any)["id"].(string)
				Expect(seen[id]).To(BeFalse(), "id %q served twice across pages", id)
				seen[id] = true
			}
		}
	})

	It("rejects a malformed cursor", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		_, status := doJSON(srv, http.MethodGet, "/api/skills?cursor=not-base64!", "", "")
		Expect(status).To(Equal(http.StatusBadRequest))
	})

	It("filters the list by scope", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "mine-skill") // author user-seed
		seedSkill(store, "team-skill")
		teamRec, err := store.GetSkill(context.Background(), "team-skill")
		Expect(err).NotTo(HaveOccurred())
		// Re-seed the team skill under a different author: author_subject is
		// preserved on update, so rebuild it from scratch.
		_, err = store.DeleteSkill(context.Background(), "team-skill")
		Expect(err).NotTo(HaveOccurred())
		teamRec.AuthorSubject = "user-other"
		_, err = store.UpsertSkill(context.Background(), *teamRec)
		Expect(err).NotTo(HaveOccurred())
		srv := newSkillsServer(store)

		body, _ := doJSON(srv, http.MethodGet, "/api/skills?scope=mine", "", "user-seed")
		items, _ := body["items"].([]any)
		Expect(items).To(HaveLen(1))
		Expect(items[0].(map[string]any)).To(HaveKeyWithValue("id", "mine-skill"))

		body, _ = doJSON(srv, http.MethodGet, "/api/skills?scope=team", "", "user-seed")
		items, _ = body["items"].([]any)
		Expect(items).To(HaveLen(1))
		Expect(items[0].(map[string]any)).To(HaveKeyWithValue("id", "team-skill"))
	})

	It("lists the skills generated from a session via ?session_id=", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "from-sess") // GeneratedFromSessionIDs: ["sess-1"]
		now := time.Now().UTC()
		_, err := store.UpsertSkill(context.Background(), storage.SkillRecord{
			ID: "unrelated", Slug: "unrelated", Name: "Other", Type: "workflow",
			GeneratedFromSessionIDs: []string{"sess-other"},
			CreatedAt:               now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		srv := newSkillsServer(store)

		body, status := doJSON(srv, http.MethodGet, "/api/skills?session_id=sess-1", "", "")
		Expect(status).To(Equal(http.StatusOK))
		items, _ := body["items"].([]any)
		Expect(items).To(HaveLen(1))
		Expect(items[0].(map[string]any)).To(HaveKeyWithValue("id", "from-sess"))
		Expect(body).NotTo(HaveKey("counts"), "the session lookup keeps the legacy unpaginated envelope")
	})

	It("saves edits via PUT and persists content", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s")
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPut, "/api/skills/s", `{"content":"# new body","name":"Renamed"}`, "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(HaveKeyWithValue("content", "# new body"))
		Expect(body).To(HaveKeyWithValue("name", "Renamed"))
		Expect(body).To(HaveKeyWithValue("slug", "renamed"), "the slug follows a rename")

		saved, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(saved.Content).To(Equal("# new body"))
	})

	It("publishes an immutable version and stamps the author", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s")
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/s/versions", `{"changelog":"first"}`, "user-pub")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("semver", "0.1.0"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-pub"))

		// second publish bumps the patch
		body, _ = doJSON(srv, http.MethodPost, "/api/skills/s/versions", `{"changelog":"second"}`, "")
		Expect(body).To(HaveKeyWithValue("semver", "0.1.1"))

		body, status = doJSON(srv, http.MethodGet, "/api/skills/s/versions", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(HaveKeyWithValue("totalCount", float64(2)))
		versions, _ := body["versions"].([]any)
		Expect(versions[0].(map[string]any)).To(HaveKeyWithValue("semver", "0.1.1"), "newest first")
	})

	It("publishes the current head when the body is empty", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s") // content "# body"
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/s/versions", "", "")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("content", "# body"))
	})

	It("rejects a malformed publish body without minting a version", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s")
		srv := newSkillsServer(store)
		for _, body := range []string{
			`{"changelog": "trunc`,        // truncated JSON
			`{"changelog":"a"}{"more":1}`, // trailing second value
		} {
			_, status := doJSON(srv, http.MethodPost, "/api/skills/s/versions", body, "")
			Expect(status).To(Equal(http.StatusBadRequest), "body %q must be rejected", body)
		}

		versions, err := store.ListSkillVersions(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(BeEmpty(), "a garbled request must not publish")
	})

	It("duplicates a skill under a fresh id attributed to the duplicator", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s")
		srv := newSkillsServer(store)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/s/duplicate", "", "user-dup")
		Expect(status).To(Equal(http.StatusCreated))
		// slug is cosmetic now, so the copy shares the parent's slug; the name
		// carries the "(copy)" marker and parentId points at the original id.
		Expect(body).To(HaveKeyWithValue("slug", "s"))
		Expect(body).To(HaveKeyWithValue("name", "Debug React Hooks (copy)"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-dup"))
		Expect(body).To(HaveKeyWithValue("parentId", "s"))
		Expect(body["id"]).NotTo(Equal("s"), "the duplicate gets its own opaque id")

		// Slug is no longer an identity, so duplicates coexist — a second
		// duplicate gets another distinct id rather than overwriting the first.
		second, status := doJSON(srv, http.MethodPost, "/api/skills/s/duplicate", "", "user-dup")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(second["id"]).NotTo(Equal(body["id"]))
	})

	It("creates a blank skill from scratch attributed to the caller", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		body, status := doJSON(srv, http.MethodPost, "/api/skills",
			`{"name":"My New Skill","description":"d"}`, "user-new")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("slug", "my-new-skill"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-new"))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", false))
		Expect(body).To(HaveKeyWithValue("originatingSessionIds", BeEmpty()))
	})

	It("deletes a skill for its creator", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s") // author_subject = user-seed
		srv := newSkillsServer(store)
		_, status := doJSON(srv, http.MethodDelete, "/api/skills/s", "", "user-seed")
		Expect(status).To(Equal(http.StatusNoContent))
		rec, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil())
	})

	It("forbids deleting a skill owned by another user", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s") // author_subject = user-seed
		srv := newSkillsServer(store)
		_, status := doJSON(srv, http.MethodDelete, "/api/skills/s", "", "user-other")
		Expect(status).To(Equal(http.StatusForbidden))
		rec, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil(), "the skill survives a forbidden delete")
	})

	It("404s when deleting a missing skill", func() {
		srv := newSkillsServer(storage.NewMemoryStore())
		_, status := doJSON(srv, http.MethodDelete, "/api/skills/missing", "", "user-x")
		Expect(status).To(Equal(http.StatusNotFound))
	})

	It("renders a drop-in SKILL.md and counts the download", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "s")
		srv := newSkillsServer(store)
		recorder := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/skills/s/skill.md", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Header().Get("Content-Type")).To(ContainSubstring("text/markdown"))
		Expect(recorder.Header().Get("Content-Disposition")).To(ContainSubstring("s.md"))
		Expect(recorder.Body.String()).To(ContainSubstring(`name: "s"`))
		Expect(recorder.Body.String()).To(ContainSubstring("# body"))

		rec, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.DownloadCount).To(Equal(int64(1)), "download is counted as a usage signal")
	})

	It("sorts by downloads when asked", func() {
		store := storage.NewMemoryStore()
		seedSkill(store, "cold")
		seedSkill(store, "hot")
		Expect(store.IncrementSkillDownloads(context.Background(), "hot")).To(Succeed())
		srv := newSkillsServer(store)

		body, status := doJSON(srv, http.MethodGet, "/api/skills?sort=downloads", "", "")
		Expect(status).To(Equal(http.StatusOK))
		items, _ := body["items"].([]any)
		Expect(items).To(HaveLen(2))
		Expect(items[0].(map[string]any)).To(HaveKeyWithValue("id", "hot"))
	})
})

// fakeQuerier serves canned trace summaries and spans, standing in for the
// core trace API the cassette reads over HTTP.
type fakeQuerier struct {
	summaries map[string][]skill.TraceSummary
	traces    map[string]*skill.Trace
	notFound  bool
}

func (q *fakeQuerier) TraceSummaries(_ context.Context, sessionID string) ([]skill.TraceSummary, error) {
	if q.notFound {
		return nil, fmt.Errorf("list traces for session %s: %w", sessionID, skill.ErrNotFound)
	}
	return q.summaries[sessionID], nil
}

func (q *fakeQuerier) Trace(_ context.Context, traceID string) (*skill.Trace, error) {
	return q.traces[traceID], nil
}

var _ = Describe("Generate skill", func() {
	// stubOpenAI answers the OpenAI chat-completions shape with a fixed
	// generated-skill JSON payload, so the whole generate path — transcript,
	// LLM call, parse, persist — runs without a network.
	stubOpenAI := func(generated map[string]any) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			content, err := json.Marshal(generated)
			Expect(err).NotTo(HaveOccurred())
			response := map[string]any{
				"choices": []any{map[string]any{"message": map[string]any{"content": string(content)}}},
			}
			w.Header().Set("Content-Type", "application/json")
			Expect(json.NewEncoder(w).Encode(response)).To(Succeed())
		}))
	}

	newGenerateServer := func(store storage.Store, querier skill.Querier, llmBase string) *server.Server {
		cfg := server.Config{
			LLM: skill.LLMCallerConfig{Provider: "openai", APIKey: "test-key", BaseURL: llmBase},
		}
		return server.New(cfg, store, querier, nil)
	}

	sessionTurns := map[string][]skill.TraceSummary{
		"sess-1": {{TraceID: "t1", UserPrompt: "fix the flaky test", ResponsePreview: "done"}},
	}

	It("generates, persists, and returns the created skill", func() {
		llm := stubOpenAI(map[string]any{
			"name":        "Diagnose Flaky Tests",
			"description": "Use when tests flake.",
			"tags":        []string{"testing"},
			"content":     "## Steps\n1. Re-run.",
		})
		defer llm.Close()

		store := storage.NewMemoryStore()
		srv := newGenerateServer(store, &fakeQuerier{summaries: sessionTurns}, llm.URL)

		body, status := doJSON(srv, http.MethodPost, "/api/skills/generate",
			`{"sessionIds":["sess-1"]}`, "user-gen")
		Expect(status).To(Equal(http.StatusCreated))
		Expect(body).To(HaveKeyWithValue("slug", "diagnose-flaky-tests"))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", true))
		Expect(body).To(HaveKeyWithValue("authorId", "user-gen"))
		Expect(body).To(HaveKeyWithValue("originatingSessionIds", ConsistOf("sess-1")))

		id, _ := body["id"].(string)
		rec, err := store.GetSkill(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil(), "the generated skill is persisted under its minted id")
	})

	It("rejects generate with no sessionIds before touching anything", func() {
		srv := newGenerateServer(storage.NewMemoryStore(), &fakeQuerier{}, "http://unused.invalid")
		body, status := doJSON(srv, http.MethodPost, "/api/skills/generate", `{"sessionIds":[]}`, "")
		Expect(status).To(Equal(http.StatusBadRequest))
		Expect(body["error"]).To(ContainSubstring("sessionIds"))
	})

	It("maps a session the core cannot see to 404", func() {
		srv := newGenerateServer(storage.NewMemoryStore(), &fakeQuerier{notFound: true}, "http://unused.invalid")
		_, status := doJSON(srv, http.MethodPost, "/api/skills/generate", `{"sessionIds":["nope"]}`, "")
		Expect(status).To(Equal(http.StatusNotFound))
	})

	It("maps an empty transcript to 422", func() {
		srv := newGenerateServer(storage.NewMemoryStore(), &fakeQuerier{}, "http://unused.invalid")
		_, status := doJSON(srv, http.MethodPost, "/api/skills/generate", `{"sessionIds":["empty-sess"]}`, "")
		Expect(status).To(Equal(http.StatusUnprocessableEntity))
	})

	It("answers 501 when no core url is configured", func() {
		srv := server.New(server.Config{}, storage.NewMemoryStore(), nil, nil)
		_, status := doJSON(srv, http.MethodPost, "/api/skills/generate", `{"sessionIds":["sess-1"]}`, "")
		Expect(status).To(Equal(http.StatusNotImplemented))
	})

	It("maps a missing provider key to 422", func() {
		GinkgoT().Setenv("OPENAI_API_KEY", "")
		GinkgoT().Setenv("ANTHROPIC_API_KEY", "")
		srv := server.New(server.Config{
			LLM: skill.LLMCallerConfig{Provider: "openai"},
		}, storage.NewMemoryStore(), &fakeQuerier{summaries: sessionTurns}, nil)
		body, status := doJSON(srv, http.MethodPost, "/api/skills/generate", `{"sessionIds":["sess-1"]}`, "")
		Expect(status).To(Equal(http.StatusUnprocessableEntity))
		Expect(body["error"]).To(ContainSubstring("API key"))
	})
})

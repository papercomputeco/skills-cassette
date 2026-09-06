package server_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

const authSubjectHeader = "x-paper-auth-subject"

// seedSkill keeps the existing external-filter specs on the revision-backed
// read path. The helper intentionally publishes through visibility metadata;
// it never populates the predecessor mutable content head.
func seedSkill(store *storage.MemoryStore, slug string) {
	_, _ = seedPublicRevision(store, slug, "user-seed", []string{"sess-1"}, []string{"react"})
}

func seedPublicRevision(
	store *storage.MemoryStore,
	slug string,
	creator string,
	sourceSessionIDs []string,
	tags []string,
) (*storage.SkillRecord, *storage.SkillRevisionRecord) {
	now := time.Now().UTC()
	skillID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("skills-cassette-test:skill:"+slug)).String()
	revisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("skills-cassette-test:revision:"+slug)).String()
	identity, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
		ID: skillID, Slug: slug, CreatorSubject: creator, CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	revision, err := store.AppendRevision(context.Background(), storage.AppendRevisionInput{
		ID: revisionID, SkillID: identity.ID, CreatorSubject: creator,
		Origin: storage.RevisionOriginManual,
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Debug React Hooks", Description: "desc", Type: "workflow",
			Tags: tags, Content: "# body", IsAIGenerated: true,
			SourceSessionIDs: sourceSessionIDs,
		},
		IdempotencyKey: "seed:" + slug, CreatedAt: now.Add(time.Millisecond),
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.SetRevisionVisibility(context.Background(), storage.SetRevisionVisibilityInput{
		SkillID: identity.ID, RevisionID: revision.ID, CallerSubject: creator,
		IsPublic: true, ChangedAt: now.Add(2 * time.Millisecond),
	})
	Expect(err).NotTo(HaveOccurred())
	return identity, revision
}

// doJSON issues a request (optionally with the gateway-authenticated subject)
// and returns the decoded response and status.
func doJSON(srv *server.Server, method, path, body, subject string) (map[string]any, int) {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	request := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if subject != "" {
		request.Header.Set(authSubjectHeader, subject)
	}
	response := httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, request)
	var decoded map[string]any
	if response.Body.Len() > 0 {
		_ = json.Unmarshal(response.Body.Bytes(), &decoded)
	}
	return decoded, response.Code
}

func newSkillsServer(store storage.Store) *server.Server {
	return server.New(server.Config{}, store, nil, nil)
}

type failingMarkdownWriter struct {
	header     http.Header
	status     int
	writeCalls int
	written    int
}

func newFailingMarkdownWriter() *failingMarkdownWriter {
	return &failingMarkdownWriter{header: make(http.Header)}
}

func (w *failingMarkdownWriter) Header() http.Header { return w.header }

func (w *failingMarkdownWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *failingMarkdownWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.writeCalls++
	written := len(body) / 2
	if written == 0 && len(body) > 0 {
		written = 1
	}
	w.written += written
	return written, errors.New("fixture response writer failed")
}

func mapKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	return keys
}

func expectLifecycleError(
	body map[string]any,
	status, wantStatus int,
	code, message string,
	resourceID ...string,
) map[string]any {
	Expect(status).To(Equal(wantStatus), "response: %#v", body)
	wantError := map[string]any{"code": code, "message": message}
	if len(resourceID) == 1 {
		wantError["resourceId"] = resourceID[0]
	}
	Expect(body).To(Equal(map[string]any{"error": wantError}))
	Expect(len(code)).To(BeNumerically("<=", 128))
	Expect(len(message)).To(BeNumerically("<=", 1024))
	return body["error"].(map[string]any)
}

var _ = Describe("unified skill revision HTTP contract", func() {
	It("revision_routes_expose_no_delete_operation", func() {
		store := storage.NewMemoryStore()
		DeferCleanup(store.Close)
		srv := newSkillsServer(store)
		ctx := context.Background()
		now := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)

		By("resolving an existing normalized identity without disclosing hidden work")
		hiddenOwner := "creator-hidden"
		viewer := "creator-viewer"
		outsider := "creator-outsider"
		identity, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "shared-skill", CreatorSubject: hiddenOwner, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		hidden, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: identity.ID, CreatorSubject: hiddenOwner,
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Do not disclose", Description: "private description", Type: "workflow",
				Tags: []string{"private-tag"}, Content: "# hidden content",
				SourceSessionIDs: []string{"hidden-session"},
			},
			IdempotencyKey: "hidden", CreatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())

		resolved, status := doJSON(srv, http.MethodPost, "/api/skills", `{"slug":"  SHARED-SKILL  "}`, viewer)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", resolved)
		Expect(resolved).To(HaveKeyWithValue("id", identity.ID))
		Expect(resolved).To(HaveKeyWithValue("slug", "shared-skill"))
		Expect(mapKeys(resolved)).To(ConsistOf(
			"id", "slug", "explicitLatestRevisionId", "createdAt",
		))
		Expect(resolved).NotTo(HaveKey("updatedAt"),
			"private appends advance mutable skill metadata and must not be observable through identity resolution")
		resolvedJSON, err := json.Marshal(resolved)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(resolvedJSON)).NotTo(ContainSubstring(hidden.ID))
		Expect(string(resolvedJSON)).NotTo(ContainSubstring("Do not disclose"))
		Expect(string(resolvedJSON)).NotTo(ContainSubstring(hiddenOwner))

		By("appending a complete private snapshot and reconciling its idempotency key")
		firstRequest := `{
			"basedOnRevisionId":null,
			"sourceRevisionId":null,
			"snapshot":{
				"name":"Viewer revision one",
				"description":"Complete first snapshot",
				"type":"workflow",
				"tags":["email","safe"],
				"content":"# revision one",
				"isAiGenerated":false,
				"sourceSessionIds":["session-one"]
			},
			"changeNote":"First explicit save",
			"idempotencyKey":"viewer-save-one"
		}`
		first, status := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/revisions", firstRequest, viewer)
		Expect(status).To(Equal(http.StatusCreated), "response: %#v", first)
		firstID := first["id"].(string)
		Expect(uuid.Validate(firstID)).To(Succeed())
		Expect(first).To(HaveKeyWithValue("skillId", identity.ID))
		Expect(first).To(HaveKeyWithValue("sequenceNumber", float64(2)))
		Expect(first).To(HaveKeyWithValue("version", "2"))
		Expect(first).NotTo(HaveKey("creatorSubject"))
		Expect(first).To(HaveKeyWithValue("basedOnRevisionId", BeNil()))
		Expect(first).To(HaveKeyWithValue("sourceRevisionId", BeNil()))
		Expect(first).To(HaveKeyWithValue("origin", "manual"))
		Expect(first).To(HaveKeyWithValue("changeNote", "First explicit save"))
		Expect(first["contentSha256"]).To(HaveLen(64))
		Expect(first["snapshot"]).To(Equal(map[string]any{
			"name": "Viewer revision one", "description": "Complete first snapshot",
			"type": "workflow", "tags": []any{"email", "safe"},
			"content": "# revision one", "isAiGenerated": false,
			"sourceSessionIds": []any{"session-one"},
		}))
		Expect(first["visibility"]).To(HaveKeyWithValue("isPublic", false))
		Expect(first).To(HaveKeyWithValue("isExplicitLatest", false))

		retried, status := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/revisions", firstRequest, viewer)
		Expect(status).To(Equal(http.StatusCreated), "response: %#v", retried)
		Expect(retried).To(HaveKeyWithValue("id", firstID))
		Expect(retried).To(HaveKeyWithValue("sequenceNumber", float64(2)))

		secondRequest := fmt.Sprintf(`{
			"basedOnRevisionId":%q,
			"snapshot":{
				"name":"Viewer revision two",
				"description":"Complete second snapshot",
				"type":"workflow",
				"tags":["email","reviewed"],
				"content":"# revision two",
				"isAiGenerated":false,
				"sourceSessionIds":["session-two"]
			},
			"changeNote":"Continue from exact UUID",
			"idempotencyKey":"viewer-save-two"
		}`, firstID)
		second, status := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/revisions", secondRequest, viewer)
		Expect(status).To(Equal(http.StatusCreated), "response: %#v", second)
		secondID := second["id"].(string)
		Expect(second).To(HaveKeyWithValue("sequenceNumber", float64(3)))
		Expect(second).To(HaveKeyWithValue("basedOnRevisionId", firstID))

		By("paging accessible history by an opaque sequence-and-UUID keyset")
		pageOne, status := doJSON(srv, http.MethodGet,
			"/api/skills/"+identity.ID+"/revisions?limit=1", "", viewer)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", pageOne)
		Expect(pageOne["items"]).To(HaveLen(1))
		Expect(pageOne["items"].([]any)[0]).To(HaveKeyWithValue("id", secondID))
		cursor, ok := pageOne["nextCursor"].(string)
		Expect(ok).To(BeTrue())
		Expect(cursor).NotTo(BeEmpty())
		pageTwo, status := doJSON(srv, http.MethodGet,
			"/api/skills/"+identity.ID+"/revisions?limit=1&cursor="+url.QueryEscape(cursor), "", viewer)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", pageTwo)
		Expect(pageTwo["items"]).To(HaveLen(1))
		Expect(pageTwo["items"].([]any)[0]).To(HaveKeyWithValue("id", firstID))
		Expect(pageTwo).NotTo(HaveKey("nextCursor"))

		By("using private content as only its creator's card, never a shared default")
		privateDetail, status := doJSON(srv, http.MethodGet, "/api/skills/"+identity.ID, "", viewer)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", privateDetail)
		Expect(privateDetail).To(HaveKeyWithValue("effectiveRevision", BeNil()))
		Expect(privateDetail["newestPrivateRevision"]).To(HaveKeyWithValue("id", secondID))
		Expect(privateDetail["cardRevision"]).To(HaveKeyWithValue("id", secondID))
		privateList, status := doJSON(srv, http.MethodGet, "/api/skills", "", viewer)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", privateList)
		Expect(privateList["items"]).To(HaveLen(1))
		Expect(privateList["items"].([]any)[0]).To(HaveKeyWithValue("id", identity.ID))

		inaccessible, status := doJSON(srv, http.MethodGet,
			"/api/skills/"+identity.ID+"/revisions/"+secondID, "", outsider)
		errorBody := expectLifecycleError(inaccessible, status, http.StatusNotFound,
			"revision_not_found", "The requested revision was not found.")
		Expect(errorBody).NotTo(HaveKey("resourceId"))
		_, status = doJSON(srv, http.MethodGet, "/api/skills/"+identity.ID, "", outsider)
		Expect(status).To(Equal(http.StatusNotFound))
		_, status = doJSON(srv, http.MethodGet, "/api/skills/"+identity.ID+"/revisions", "", outsider)
		Expect(status).To(Equal(http.StatusNotFound))

		By("projecting the public effective revision separately from newer private continuation")
		_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
			SkillID: identity.ID, RevisionID: firstID, CallerSubject: viewer,
			IsPublic: true, ChangedAt: now.Add(4 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		continued, status := doJSON(srv, http.MethodGet, "/api/skills/"+identity.ID, "", viewer)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", continued)
		Expect(continued["effectiveRevision"]).To(HaveKeyWithValue("id", firstID))
		Expect(continued["newestPrivateRevision"]).To(HaveKeyWithValue("id", secondID))
		Expect(continued["cardRevision"]).To(HaveKeyWithValue("id", firstID))
		Expect(continued).To(HaveKeyWithValue("hasNewerPrivateRevision", true))

		shared, status := doJSON(srv, http.MethodGet, "/api/skills/"+identity.ID, "", outsider)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", shared)
		Expect(shared["effectiveRevision"]).To(HaveKeyWithValue("id", firstID))
		Expect(shared).To(HaveKeyWithValue("newestPrivateRevision", BeNil()))
		Expect(shared).To(HaveKeyWithValue("hasNewerPrivateRevision", false))
		sharedJSON, err := json.Marshal(shared)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(sharedJSON)).NotTo(ContainSubstring(secondID))
		Expect(string(sharedJSON)).NotTo(ContainSubstring("revision two"))

		By("paging one effective card per stable skill with a camelCase keyset cursor")
		pageBSkill, pageBFirst := seedPublicRevision(store, "page-b", viewer, nil, []string{"page"})
		pageCSkill, pageCRevision := seedPublicRevision(store, "page-c", viewer, nil, []string{"page"})
		pageBSecond, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: pageBSkill.ID, CreatorSubject: viewer,
			BasedOnRevisionID: pageBFirst.ID, Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Page B newest", Description: "Only this revision may become the card.",
				Type: "workflow", Tags: []string{"page"}, Content: "# Page B newest",
				SourceSessionIDs: []string{},
			},
			IdempotencyKey: "page-b-newest", CreatedAt: now.Add(10 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
			SkillID: pageBSkill.ID, RevisionID: pageBSecond.ID, CallerSubject: viewer,
			IsPublic: true, ChangedAt: now.Add(11 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())

		wantCards := map[string]string{
			identity.ID: firstID, pageBSkill.ID: pageBSecond.ID, pageCSkill.ID: pageCRevision.ID,
		}
		seenSkills := map[string]struct{}{}
		cursor = ""
		for pageNumber := range len(wantCards) {
			path := "/api/skills?limit=1"
			if cursor != "" {
				path += "&cursor=" + url.QueryEscape(cursor)
			}
			page, pageStatus := doJSON(srv, http.MethodGet, path, "", viewer)
			Expect(pageStatus).To(Equal(http.StatusOK), "page %d response: %#v", pageNumber+1, page)
			Expect(page).To(HaveKeyWithValue("counts", map[string]any{
				"all": float64(3), "mine": float64(3), "team": float64(0),
			}))
			items := page["items"].([]any)
			Expect(items).To(HaveLen(1))
			card := items[0].(map[string]any)
			skillID := card["id"].(string)
			Expect(seenSkills).NotTo(HaveKey(skillID), "a skill with several revisions must still yield one card")
			seenSkills[skillID] = struct{}{}
			Expect(card["cardRevision"]).To(HaveKeyWithValue("id", wantCards[skillID]))
			if pageNumber < len(wantCards)-1 {
				cursor, ok = page["nextCursor"].(string)
				Expect(ok).To(BeTrue())
				Expect(cursor).NotTo(BeEmpty())
			} else {
				Expect(page).NotTo(HaveKey("nextCursor"))
			}
		}
		Expect(seenSkills).To(HaveLen(3))

		By("binding every effective-skill cursor to the sort that produced it")
		Expect(cursor).NotTo(BeEmpty(), "the last page boundary is a recent-order cursor")
		replayed, replayedStatus := doJSON(srv, http.MethodGet,
			"/api/skills?limit=1&sort=downloads&cursor="+url.QueryEscape(cursor), "", viewer)
		expectLifecycleError(replayed, replayedStatus, http.StatusBadRequest,
			"invalid_request", "The skills cursor is invalid.")
		recentAgain, recentStatus := doJSON(srv, http.MethodGet,
			"/api/skills?limit=1&sort=recent&cursor="+url.QueryEscape(cursor), "", viewer)
		Expect(recentStatus).To(Equal(http.StatusOK), "response: %#v", recentAgain)
		downloadsPage, downloadsStatus := doJSON(srv, http.MethodGet, "/api/skills?limit=1&sort=downloads", "", viewer)
		Expect(downloadsStatus).To(Equal(http.StatusOK), "response: %#v", downloadsPage)
		downloadsCursor, ok := downloadsPage["nextCursor"].(string)
		Expect(ok).To(BeTrue())
		downloadsNext, downloadsNextStatus := doJSON(srv, http.MethodGet,
			"/api/skills?limit=1&sort=downloads&cursor="+url.QueryEscape(downloadsCursor), "", viewer)
		Expect(downloadsNextStatus).To(Equal(http.StatusOK), "response: %#v", downloadsNext)
		Expect(downloadsNext["items"]).To(HaveLen(1))
		mismatched, mismatchedStatus := doJSON(srv, http.MethodGet,
			"/api/skills?limit=1&cursor="+url.QueryEscape(downloadsCursor), "", viewer)
		expectLifecycleError(mismatched, mismatchedStatus, http.StatusBadRequest,
			"invalid_request", "The skills cursor is invalid.")

		By("keeping every emitted cursor decodable by the shape earlier releases accept")
		// legacySkillsCursor is the cursor struct before sort binding, decoded
		// with the same strictness those releases apply: unknown keys are
		// rejected, ts must be non-zero, dc must be non-negative.
		type legacySkillsCursor struct {
			UpdatedAt time.Time `json:"ts"`
			Downloads int64     `json:"dc"`
			ID        string    `json:"id"`
		}
		decodeLegacy := func(token string) legacySkillsCursor {
			raw, err := base64.RawURLEncoding.DecodeString(token)
			Expect(err).NotTo(HaveOccurred())
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			var legacy legacySkillsCursor
			Expect(decoder.Decode(&legacy)).To(Succeed(), "an earlier release must decode %s", string(raw))
			Expect(legacy.UpdatedAt.IsZero()).To(BeFalse())
			Expect(legacy.Downloads).To(BeNumerically(">=", 0))
			_, err = uuid.Parse(legacy.ID)
			Expect(err).NotTo(HaveOccurred())
			return legacy
		}
		recentLegacy := decodeLegacy(cursor)
		downloadsLegacy := decodeLegacy(downloadsCursor)

		By("accepting a legacy cursor that recorded no sort under either sort")
		encodeLegacy := func(legacy legacySkillsCursor) string {
			raw, err := json.Marshal(legacy)
			Expect(err).NotTo(HaveOccurred())
			return base64.RawURLEncoding.EncodeToString(raw)
		}
		legacyCursor := encodeLegacy(legacySkillsCursor{
			UpdatedAt: recentLegacy.UpdatedAt, Downloads: downloadsLegacy.Downloads, ID: recentLegacy.ID,
		})
		for _, sort := range []string{"", "&sort=recent", "&sort=downloads"} {
			page, pageStatus := doJSON(srv, http.MethodGet,
				"/api/skills?limit=1"+sort+"&cursor="+url.QueryEscape(legacyCursor), "", viewer)
			Expect(pageStatus).To(Equal(http.StatusOK), "sort %q response: %#v", sort, page)
		}
		for _, skillID := range []string{identity.ID, pageBSkill.ID, pageCSkill.ID} {
			Expect(seenSkills).To(HaveKey(skillID))
		}

		By("rejecting both skill and saved-revision deletion without deleting history")
		_, status = doJSON(srv, http.MethodDelete,
			"/api/skills/"+identity.ID+"/revisions/"+firstID, "", viewer)
		Expect(status).To(Equal(http.StatusMethodNotAllowed))
		_, status = doJSON(srv, http.MethodDelete, "/api/skills/"+identity.ID, "", viewer)
		Expect(status).To(Equal(http.StatusMethodNotAllowed))
		retained, err := store.GetRevision(ctx, storage.RevisionReadOpts{
			SkillID: identity.ID, RevisionID: firstID, CallerSubject: viewer,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(retained).NotTo(BeNil())
	})

	It("revision_contract_does_not_reinterpret_labels_as_tags", func() {
		store := &effectiveFilterCaptureStore{
			MemoryStore:      storage.NewMemoryStore(),
			attachmentView:   "fixture.revision_labels",
			attachmentLabels: map[string][]string{},
		}
		DeferCleanup(store.Close)
		filters, err := server.ParseExternalFilters(
			`[{"param":"label","view":"fixture.revision_labels","type_value":"skill","normalize":["trim","casefold"]}]`,
		)
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()
		identity, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "snapshot-tags", CreatorSubject: "tag-owner", CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		appendBody := `{
			"snapshot":{
				"name":"Tagged snapshot",
				"description":"Tags are immutable content, not mutable revision labels.",
				"type":"workflow",
				"tags":["email","safety"],
				"content":"# tagged",
				"isAiGenerated":false,
				"sourceSessionIds":[]
			},
			"changeNote":"Save tags with content",
			"idempotencyKey":"tagged-save"
		}`
		created, status := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/revisions", appendBody, "tag-owner")
		Expect(status).To(Equal(http.StatusCreated), "response: %#v", created)
		revisionID := created["id"].(string)
		originalHash := created["contentSha256"]
		originalSnapshot := created["snapshot"]
		Expect(originalSnapshot).To(HaveKeyWithValue("tags", []any{"email", "safety"}))
		Expect(originalSnapshot.(map[string]any)).NotTo(HaveKey("labels"))

		// The external attachment label deliberately differs from the target's
		// immutable revision tags. A decoy carries the same word as a revision
		// tag but has no attachment, proving the two namespaces are not joined.
		decoy, _ := seedPublicRevision(store.MemoryStore, "tag-shaped-decoy", "decoy-owner", nil,
			[]string{"triage-board"})
		store.attachmentLabels[identity.ID] = []string{"triage-board"}

		for _, request := range []struct {
			method string
			path   string
			body   string
		}{
			{http.MethodPut, "/api/skills/" + identity.ID + "/revisions/" + revisionID, `{"tags":["mutated"]}`},
			{http.MethodPut, "/api/skills/" + identity.ID + "/revisions/" + revisionID + "/tags", `{"tags":["mutated"]}`},
			{http.MethodPost, "/api/skills/" + identity.ID + "/revisions/" + revisionID + "/labels", `{"label":"latest-ish"}`},
		} {
			_, requestStatus := doJSON(srv, request.method, request.path, request.body, "tag-owner")
			Expect(requestStatus).To(Or(Equal(http.StatusNotFound), Equal(http.StatusMethodNotAllowed)),
				"%s %s must not expose mutable revision tags", request.method, request.path)
		}

		visibility, status := doJSON(srv, http.MethodPut,
			"/api/skills/"+identity.ID+"/revisions/"+revisionID+"/visibility",
			`{"isPublic":true}`, "tag-owner")
		Expect(status).To(Equal(http.StatusOK), "response: %#v", visibility)
		latest, status := doJSON(srv, http.MethodPut, "/api/skills/"+identity.ID+"/latest",
			fmt.Sprintf(`{"revisionId":%q}`, revisionID), "tag-owner")
		Expect(status).To(Equal(http.StatusOK), "response: %#v", latest)

		By("selecting through the external attachment label without consulting revision tags")
		selected, status := doJSON(srv, http.MethodGet,
			"/api/skills?label=%20TRIAGE-BOARD%20", "", "tag-owner")
		Expect(status).To(Equal(http.StatusOK), "response: %#v", selected)
		Expect(store.listOpts).NotTo(BeNil())
		Expect(store.listOpts.External).To(Equal([]storage.ExternalAttachmentFilter{{
			View: "fixture.revision_labels", TypeValue: "skill", Values: []string{"triage-board"},
		}}))
		Expect(selected["items"]).To(HaveLen(1))
		selectedCard := selected["items"].([]any)[0].(map[string]any)
		Expect(selectedCard).To(HaveKeyWithValue("id", identity.ID))
		Expect(selectedCard).NotTo(HaveKeyWithValue("id", decoy.ID))
		Expect(selectedCard["cardRevision"].(map[string]any)["snapshot"]).
			To(HaveKeyWithValue("tags", []any{"email", "safety"}))

		unchanged, status := doJSON(srv, http.MethodGet,
			"/api/skills/"+identity.ID+"/revisions/"+revisionID, "", "tag-owner")
		Expect(status).To(Equal(http.StatusOK), "response: %#v", unchanged)
		Expect(unchanged).To(HaveKeyWithValue("id", revisionID))
		Expect(unchanged).To(HaveKeyWithValue("contentSha256", originalHash))
		Expect(unchanged).To(HaveKeyWithValue("snapshot", originalSnapshot))

		openapi, status := doJSON(srv, http.MethodGet, "/openapi", "", "")
		Expect(status).To(Equal(http.StatusOK))
		paths := openapi["paths"].(map[string]any)
		for path := range paths {
			Expect(path).NotTo(HaveSuffix("/tags"))
			Expect(path).NotTo(HaveSuffix("/labels"))
		}
		appendOperation := paths["/api/skills/{skillId}/revisions"].(map[string]any)["post"].(map[string]any)
		requestSchema := appendOperation["requestBody"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
		snapshotSchema := requestSchema["properties"].(map[string]any)["snapshot"].(map[string]any)
		Expect(snapshotSchema["properties"]).To(HaveKey("tags"))
		Expect(snapshotSchema["properties"]).NotTo(HaveKey("labels"))
		Expect(snapshotSchema).To(HaveKeyWithValue("additionalProperties", false))
	})

	It("current_main_regression_suite_preserves_distribution_protections", func() {
		store := &effectiveFilterCaptureStore{MemoryStore: storage.NewMemoryStore()}
		DeferCleanup(store.Close)
		provenanceSkill, provenanceRevision := seedPublicRevision(
			store.MemoryStore, "provenance-skill", "owner-a",
			[]string{"evidence-session"}, []string{"incident", "safe"},
		)
		seedPublicRevision(store.MemoryStore, "unrelated-skill", "owner-b",
			[]string{"another-session"}, []string{"other"})

		filters, err := server.ParseExternalFilters(`[{"param":"label","view":"fixture.attachments","type_value":"skill","normalize":["trim","casefold"]}]`)
		Expect(err).NotTo(HaveOccurred())
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		By("keeping deployment-armed filters inside both effective row and count queries")
		listed, status := doJSON(srv, http.MethodGet, "/api/skills?label=%20SAFE%20", "", "viewer")
		Expect(status).To(Equal(http.StatusOK), "response: %#v", listed)
		Expect(store.listOpts).NotTo(BeNil())
		Expect(store.countOpts).NotTo(BeNil())
		wantFilter := []storage.ExternalAttachmentFilter{{
			View: "fixture.attachments", TypeValue: "skill", Values: []string{"safe"},
		}}
		Expect(store.listOpts.External).To(Equal(wantFilter))
		Expect(store.countOpts.External).To(Equal(wantFilter))
		store.listErr = fmt.Errorf("effective attachment filter: %w", storage.ErrExternalViewUnavailable)
		unavailable, status := doJSON(srv, http.MethodGet, "/api/skills?label=safe", "", "viewer")
		Expect(status).To(Equal(http.StatusServiceUnavailable), "response: %#v", unavailable)
		Expect(unavailable).To(HaveKey("error"))
		Expect(unavailable).NotTo(HaveKey("items"), "an armed broken filter must never leak unfiltered rows")
		store.listErr = nil

		By("preserving accessible revision provenance reverse lookup")
		bySession, status := doJSON(srv, http.MethodGet,
			"/api/skills?session_id=evidence-session", "", "viewer")
		Expect(status).To(Equal(http.StatusOK), "response: %#v", bySession)
		Expect(bySession).NotTo(HaveKey("counts"))
		Expect(store.sessionOpts).NotTo(BeNil())
		Expect(store.sessionOpts.Limit).To(Equal(100),
			"the handler must bound session selection before storage materializes projections")
		Expect(bySession["items"]).To(HaveLen(1))
		Expect(bySession["items"].([]any)[0]).To(HaveKeyWithValue("id", provenanceSkill.ID))
		Expect(bySession["items"].([]any)[0].(map[string]any)["effectiveRevision"]).
			To(HaveKeyWithValue("id", provenanceRevision.ID))

		By("always rendering the public effective revision, even for its creator")
		newerPrivate, err := store.AppendRevision(context.Background(), storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: provenanceSkill.ID, CreatorSubject: "owner-a",
			BasedOnRevisionID: provenanceRevision.ID, Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Unreleased private continuation", Description: "Must never be distributed.",
				Type: "workflow", Tags: []string{"private-only"}, Content: "# PRIVATE DO NOT SERVE",
				SourceSessionIDs: []string{"private-session"},
			},
			IdempotencyKey: "private-continuation", CreatedAt: time.Now().UTC().Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		request := httptest.NewRequest(http.MethodGet,
			"/api/skills/"+provenanceSkill.ID+"/skill.md", nil)
		request.Header.Set(authSubjectHeader, "owner-a")
		response := httptest.NewRecorder()
		srv.Handler().ServeHTTP(response, request)
		Expect(response.Code).To(Equal(http.StatusOK), response.Body.String())
		Expect(response.Header().Get("Content-Type")).To(ContainSubstring("text/markdown"))
		Expect(response.Header().Get("Content-Disposition")).To(ContainSubstring("provenance-skill.md"))
		Expect(response.Body.String()).To(ContainSubstring(`name: "provenance-skill"`))
		Expect(response.Body.String()).To(ContainSubstring(`version: "1"`))
		Expect(response.Body.String()).To(ContainSubstring(`tags: ["incident","safe"]`))
		Expect(response.Body.String()).To(ContainSubstring("# body"))
		Expect(response.Body.String()).NotTo(ContainSubstring(newerPrivate.ID))
		Expect(response.Body.String()).NotTo(ContainSubstring("PRIVATE DO NOT SERVE"))
		Expect(response.Body.String()).NotTo(ContainSubstring("private-only"))
		persisted, err := store.GetSkill(context.Background(), provenanceSkill.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(persisted.DownloadCount).To(Equal(int64(1)))

		By("counting a download only after the complete markdown body is written")
		failedWriter := newFailingMarkdownWriter()
		failedRequest := httptest.NewRequest(http.MethodGet,
			"/api/skills/"+provenanceSkill.ID+"/skill.md", nil)
		failedRequest.Header.Set(authSubjectHeader, "owner-a")
		srv.Handler().ServeHTTP(failedWriter, failedRequest)
		Expect(failedWriter.writeCalls).To(Equal(1))
		Expect(failedWriter.written).To(BeNumerically(">", 0))
		persisted, err = store.GetSkill(context.Background(), provenanceSkill.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(persisted.DownloadCount).To(Equal(int64(1)),
			"a partial writer failure must not increment the download count")

		By("rejecting a creator's private-only skill from the unversioned distribution route")
		privateSkill, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "private-download", CreatorSubject: "owner-a", CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.AppendRevision(context.Background(), storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: privateSkill.ID, CreatorSubject: "owner-a",
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Private download", Description: "No public revision.", Type: "workflow",
				Tags: []string{}, Content: "# NEVER PUBLIC", SourceSessionIDs: []string{},
			},
			IdempotencyKey: "private-download", CreatedAt: time.Now().UTC().Add(time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		privateBody, privateStatus := doJSON(srv, http.MethodGet,
			"/api/skills/"+privateSkill.ID+"/skill.md", "", "owner-a")
		expectLifecycleError(privateBody, privateStatus, http.StatusConflict,
			"revision_private", "A private revision cannot be used by this public operation.")
		privatePersisted, err := store.GetSkill(context.Background(), privateSkill.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(privatePersisted.DownloadCount).To(BeZero())

		By("serving public content when the best-effort download counter fails")
		store.incrementErr = errors.New("fixture download counter unavailable")
		response = httptest.NewRecorder()
		srv.Handler().ServeHTTP(response, request)
		Expect(response.Code).To(Equal(http.StatusOK), response.Body.String())
		Expect(response.Body.String()).To(ContainSubstring("# body"))
		Expect(response.Body.String()).NotTo(ContainSubstring("PRIVATE DO NOT SERVE"))
		persisted, err = store.GetSkill(context.Background(), provenanceSkill.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(persisted.DownloadCount).To(Equal(int64(1)), "counter failure must not change content success or the prior count")

		By("retaining one stamped release identity in OpenAPI and its cassette extension")
		document, status := doJSON(srv, http.MethodGet, "/openapi", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(document["info"]).To(HaveKeyWithValue("version", server.Version))
		manifest := document["x-tapes-cassette"].(map[string]any)
		cassette := manifest["cassette"].(map[string]any)
		Expect(cassette).To(HaveKeyWithValue("version", server.Version))
		Expect(cassette).To(HaveKeyWithValue("image",
			"public.ecr.aws/g4e5l3z3/papercomputeco/skills-cassette:v"+server.Version))
	})

	It("renders_a_maximum_unicode_append_as_skill_markdown", func() {
		const (
			formerMarkdownLimit = 8 << 20
			renderedLimit       = 12 << 20
			requestLimit        = 13 << 20
		)

		store := storage.NewMemoryStore()
		DeferCleanup(store.Close)
		srv := newSkillsServer(store)
		owner := "markdown-boundary-owner"
		slug := strings.Repeat("s", 128)
		identity, status := doJSON(srv, http.MethodPost, "/api/skills",
			fmt.Sprintf(`{"slug":%q}`, slug), owner)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", identity)
		skillID := identity["id"].(string)

		maximumUniqueValues := func(count int, suffixBase rune) []string {
			values := make([]string, count)
			for index := range count {
				values[index] = strings.Repeat("<", 255) + string(suffixBase+rune(index))
			}
			return values
		}
		requestBody, err := json.Marshal(map[string]any{
			"snapshot": map[string]any{
				"name": strings.Repeat("<", 1024), "description": strings.Repeat("<", 1<<20),
				"type": "workflow", "tags": maximumUniqueValues(64, '\u0400'),
				"content": strings.Repeat("😀", 1<<20), "isAiGenerated": false,
				"sourceSessionIds": maximumUniqueValues(100, '\u1000'),
			},
			"changeNote": strings.Repeat("<", 1024), "idempotencyKey": strings.Repeat("<", 256),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(requestBody)).To(BeNumerically("<", requestLimit),
			"the all-fields-at-limit fixture must remain admissible under the transport bound")

		appendRequest := httptest.NewRequest(http.MethodPost,
			"/api/skills/"+skillID+"/revisions", bytes.NewReader(requestBody))
		appendRequest.Header.Set(authSubjectHeader, owner)
		appendResponse := httptest.NewRecorder()
		srv.Handler().ServeHTTP(appendResponse, appendRequest)
		Expect(appendResponse.Code).To(Equal(http.StatusCreated), appendResponse.Body.String())
		var created struct {
			ID string `json:"id"`
		}
		Expect(json.Unmarshal(appendResponse.Body.Bytes(), &created)).To(Succeed())
		Expect(created.ID).NotTo(BeEmpty())

		visibility, status := doJSON(srv, http.MethodPut,
			"/api/skills/"+skillID+"/revisions/"+created.ID+"/visibility",
			`{"isPublic":true}`, owner)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", visibility)

		downloadRequest := httptest.NewRequest(http.MethodGet, "/api/skills/"+skillID+"/skill.md", nil)
		downloadResponse := httptest.NewRecorder()
		srv.Handler().ServeHTTP(downloadResponse, downloadRequest)
		Expect(downloadResponse.Code).To(Equal(http.StatusOK), downloadResponse.Body.String())
		Expect(downloadResponse.Body.Len()).To(And(
			BeNumerically(">", formerMarkdownLimit),
			BeNumerically("<", renderedLimit),
		), "maximum-width content and maximally escaped frontmatter must fit the rendered cap")
		Expect(bytes.HasSuffix(downloadResponse.Body.Bytes(), []byte("😀\n"))).To(BeTrue())

		document, status := doJSON(srv, http.MethodGet, "/openapi", "", "")
		Expect(status).To(Equal(http.StatusOK))
		paths := document["paths"].(map[string]any)
		operation := paths["/api/skills/{skillId}/skill.md"].(map[string]any)["get"].(map[string]any)
		response := operation["responses"].(map[string]any)["200"].(map[string]any)
		content := response["content"].(map[string]any)["text/markdown"].(map[string]any)
		schema := content["schema"].(map[string]any)
		Expect(schema).To(And(
			HaveKeyWithValue("maxLength", float64(renderedLimit)),
			HaveKeyWithValue("x-maxBytes", float64(renderedLimit)),
		))
	})

	It("revision_conflicts_return_stable_error_envelopes", func() {
		store := storage.NewMemoryStore()
		DeferCleanup(store.Close)
		ctx := context.Background()
		owner := "conflict-owner"
		identity, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "conflict-skill", CreatorSubject: owner, CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		revision, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: identity.ID, CreatorSubject: owner,
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Conflict target", Description: "private", Type: "workflow",
				Tags: []string{"stable"}, Content: "# original", SourceSessionIDs: []string{},
			},
			IdempotencyKey: "conflict-key", CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		srv := newSkillsServer(store)
		coveredDSG46Codes := map[string]struct{}{}
		expectDSG46Error := func(
			body map[string]any,
			status, wantStatus int,
			code, message string,
			resourceID ...string,
		) map[string]any {
			coveredDSG46Codes[code] = struct{}{}
			return expectLifecycleError(body, status, wantStatus, code, message, resourceID...)
		}

		unauthenticated, status := doJSON(srv, http.MethodPost, "/api/skills", `{"slug":"new"}`, "")
		expectLifecycleError(unauthenticated, status, http.StatusUnauthorized,
			"unauthenticated", "Authentication is required.")

		inaccessible, status := doJSON(srv, http.MethodGet,
			"/api/skills/"+identity.ID+"/revisions/"+revision.ID, "", "other-subject")
		notFound := expectDSG46Error(inaccessible, status, http.StatusNotFound,
			"revision_not_found", "The requested revision was not found.")
		Expect(notFound).NotTo(HaveKey("resourceId"))
		inaccessibleJSON, err := json.Marshal(inaccessible)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(inaccessibleJSON)).NotTo(ContainSubstring(revision.ID))
		Expect(string(inaccessibleJSON)).NotTo(ContainSubstring("Conflict target"))

		By("defining revision_private at an owner-visible, public-only distribution boundary")
		// Exact private reads remain valid for the creator and concealed from
		// everyone else. The unversioned SKILL.md route is different: it is a
		// public distribution operation and must refuse, rather than serve, the
		// creator's only private revision with the reserved DSG-46 code.
		privateDistribution, status := doJSON(srv, http.MethodGet,
			"/api/skills/"+identity.ID+"/skill.md", "", owner)
		expectDSG46Error(privateDistribution, status, http.StatusConflict,
			"revision_private", "A private revision cannot be used by this public operation.")

		By("returning exact bounded envelopes for invalid requests, missing skills, and malformed cursors")
		missingSkillID := uuid.NewString()
		errorCases := []struct {
			name    string
			method  string
			path    string
			body    string
			status  int
			code    string
			message string
		}{
			{
				name: "invalid request body", method: http.MethodPost,
				path: "/api/skills/" + identity.ID + "/revisions", body: `{"snapshot":`,
				status: http.StatusBadRequest, code: "invalid_request", message: "The request body is invalid.",
			},
			{
				name: "missing skill", method: http.MethodGet, path: "/api/skills/" + missingSkillID,
				status: http.StatusNotFound, code: "skill_not_found", message: "The requested skill was not found.",
			},
			{
				name: "malformed effective-list cursor", method: http.MethodGet, path: "/api/skills?cursor=not-base64!",
				status: http.StatusBadRequest, code: "invalid_request", message: "The skills cursor is invalid.",
			},
			{
				name: "malformed revision cursor", method: http.MethodGet,
				path:   "/api/skills/" + identity.ID + "/revisions?cursor=not-base64!",
				status: http.StatusBadRequest, code: "invalid_request", message: "The revision cursor is invalid.",
			},
			{
				name: "malformed generation cursor", method: http.MethodGet,
				path:   "/api/skills/" + identity.ID + "/generations?cursor=not-base64!",
				status: http.StatusBadRequest, code: "invalid_request", message: "The generation cursor is invalid.",
			},
		}
		for _, testCase := range errorCases {
			responseBody, responseStatus := doJSON(srv, testCase.method, testCase.path, testCase.body, owner)
			expectLifecycleError(responseBody, responseStatus, testCase.status, testCase.code, testCase.message)
		}

		By("concealing inaccessible and foreign base/source lineage behind the same exact envelope")
		foreignSkill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "foreign-lineage", CreatorSubject: owner, CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		foreignRevision, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: foreignSkill.ID, CreatorSubject: owner,
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Accessible foreign revision", Description: "Wrong skill relationship.", Type: "workflow",
				Tags: []string{}, Content: "# foreign", SourceSessionIDs: []string{},
			},
			IdempotencyKey: "foreign-lineage", CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
			SkillID: foreignSkill.ID, RevisionID: foreignRevision.ID, CallerSubject: owner,
			IsPublic: true, ChangedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		hiddenSameSkill, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: identity.ID, CreatorSubject: "hidden-lineage-owner",
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Hidden same-skill revision", Description: "Inaccessible base.", Type: "workflow",
				Tags: []string{}, Content: "# hidden same", SourceSessionIDs: []string{},
			},
			IdempotencyKey: "hidden-same", CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		hiddenForeignSkill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "hidden-foreign-lineage", CreatorSubject: "hidden-lineage-owner",
			CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		hiddenForeignRevision, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: hiddenForeignSkill.ID, CreatorSubject: "hidden-lineage-owner",
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Hidden foreign revision", Description: "Inaccessible source.", Type: "workflow",
				Tags: []string{}, Content: "# hidden foreign", SourceSessionIDs: []string{},
			},
			IdempotencyKey: "hidden-foreign", CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())

		lineageBody := func(field, revisionID, idempotencyKey string) string {
			requestBody := map[string]any{
				field: revisionID,
				"snapshot": map[string]any{
					"name": "Rejected lineage", "description": "Must not append.", "type": "workflow",
					"tags": []string{}, "content": "# rejected", "isAiGenerated": false,
					"sourceSessionIds": []string{},
				},
				"idempotencyKey": idempotencyKey,
			}
			encoded, marshalErr := json.Marshal(requestBody)
			Expect(marshalErr).NotTo(HaveOccurred())
			return string(encoded)
		}
		lineageCases := []struct {
			name, field, revisionID, key string
		}{
			{name: "inaccessible base", field: "basedOnRevisionId", revisionID: hiddenSameSkill.ID, key: "inaccessible-base"},
			{name: "foreign base", field: "basedOnRevisionId", revisionID: foreignRevision.ID, key: "foreign-base"},
			{name: "inaccessible source", field: "sourceRevisionId", revisionID: hiddenForeignRevision.ID, key: "inaccessible-source"},
			{name: "same-skill source", field: "sourceRevisionId", revisionID: revision.ID, key: "same-skill-source"},
		}
		for _, testCase := range lineageCases {
			responseBody, responseStatus := doJSON(srv, http.MethodPost,
				"/api/skills/"+identity.ID+"/revisions",
				lineageBody(testCase.field, testCase.revisionID, testCase.key), owner)
			expectLifecycleError(responseBody, responseStatus, http.StatusNotFound,
				"revision_not_found", "The requested revision was not found.")
			encoded, marshalErr := json.Marshal(responseBody)
			Expect(marshalErr).NotTo(HaveOccurred())
			Expect(string(encoded)).NotTo(ContainSubstring(testCase.revisionID), testCase.name)
		}

		By("rejecting latest until the independent visibility operation succeeds")
		privateLatest, status := doJSON(srv, http.MethodPut, "/api/skills/"+identity.ID+"/latest",
			fmt.Sprintf(`{"revisionId":%q}`, revision.ID), owner)
		latestError := expectDSG46Error(privateLatest, status, http.StatusConflict,
			"revision_not_public", "Only a public revision can be marked latest.", revision.ID)
		Expect(latestError).To(HaveKeyWithValue("resourceId", revision.ID))
		stillPrivate, err := store.GetRevision(ctx, storage.RevisionReadOpts{
			SkillID: identity.ID, RevisionID: revision.ID, CallerSubject: owner,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(stillPrivate.Visibility.IsPublic).To(BeFalse())

		visible, status := doJSON(srv, http.MethodPut,
			"/api/skills/"+identity.ID+"/revisions/"+revision.ID+"/visibility",
			`{"isPublic":true}`, owner)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", visible)
		Expect(visible).To(And(
			HaveKeyWithValue("revisionId", revision.ID),
			HaveKeyWithValue("isPublic", true),
			HaveKey("changedAt"),
		))
		Expect(mapKeys(visible)).To(ConsistOf("revisionId", "isPublic", "changedAt"))
		visibleRetry, retryStatus := doJSON(srv, http.MethodPut,
			"/api/skills/"+identity.ID+"/revisions/"+revision.ID+"/visibility",
			`{"isPublic":true}`, owner)
		Expect(retryStatus).To(Equal(http.StatusOK), "response: %#v", visibleRetry)
		Expect(visibleRetry).To(Equal(visible), "retrying visibility=true must not rewrite audit metadata")

		latest, status := doJSON(srv, http.MethodPut, "/api/skills/"+identity.ID+"/latest",
			fmt.Sprintf(`{"revisionId":%q}`, revision.ID), owner)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", latest)
		Expect(latest).To(HaveKeyWithValue("explicitLatestRevisionId", revision.ID))
		Expect(latest["effectiveRevision"]).To(HaveKeyWithValue("id", revision.ID))
		Expect(latest["effectiveRevision"]).To(HaveKeyWithValue("isExplicitLatest", true))
		resolvedPublic, resolvedStatus := doJSON(srv, http.MethodPost, "/api/skills",
			`{"slug":"conflict-skill"}`, "identity-viewer")
		Expect(resolvedStatus).To(Equal(http.StatusOK), "response: %#v", resolvedPublic)
		Expect(resolvedPublic).To(HaveKeyWithValue("explicitLatestRevisionId", revision.ID))
		Expect(mapKeys(resolvedPublic)).To(ConsistOf(
			"id", "slug", "explicitLatestRevisionId", "createdAt",
		))
		latestRetry, retryStatus := doJSON(srv, http.MethodPut, "/api/skills/"+identity.ID+"/latest",
			fmt.Sprintf(`{"revisionId":%q}`, revision.ID), owner)
		Expect(retryStatus).To(Equal(http.StatusOK), "response: %#v", latestRetry)
		Expect(latestRetry).To(Equal(latest), "retrying the same explicit latest must be idempotent")

		privacyConflict, status := doJSON(srv, http.MethodPut,
			"/api/skills/"+identity.ID+"/revisions/"+revision.ID+"/visibility",
			`{"isPublic":false}`, owner)
		expectDSG46Error(privacyConflict, status, http.StatusConflict,
			"latest_revision_conflict", "Move or clear latest before making this revision private.", revision.ID)

		cleared, status := doJSON(srv, http.MethodDelete, "/api/skills/"+identity.ID+"/latest", "", owner)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", cleared)
		Expect(cleared).To(HaveKeyWithValue("explicitLatestRevisionId", BeNil()))
		Expect(cleared["effectiveRevision"]).To(HaveKeyWithValue("id", revision.ID))
		Expect(cleared["effectiveRevision"]).To(HaveKeyWithValue("isExplicitLatest", false))
		clearedRetry, retryStatus := doJSON(srv, http.MethodDelete,
			"/api/skills/"+identity.ID+"/latest", "", owner)
		Expect(retryStatus).To(Equal(http.StatusOK), "response: %#v", clearedRetry)
		Expect(clearedRetry).To(Equal(cleared), "clearing an already-clear latest pointer must be idempotent")
		madePrivate, status := doJSON(srv, http.MethodPut,
			"/api/skills/"+identity.ID+"/revisions/"+revision.ID+"/visibility",
			`{"isPublic":false}`, owner)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", madePrivate)
		madePrivateRetry, retryStatus := doJSON(srv, http.MethodPut,
			"/api/skills/"+identity.ID+"/revisions/"+revision.ID+"/visibility",
			`{"isPublic":false}`, owner)
		Expect(retryStatus).To(Equal(http.StatusOK), "response: %#v", madePrivateRetry)
		Expect(madePrivateRetry).To(Equal(madePrivate), "retrying visibility=false must not rewrite audit metadata")

		By("mapping conflicting idempotency identity to one bounded typed envelope")
		conflictingAppend := `{
			"snapshot":{
				"name":"Conflict target",
				"description":"private",
				"type":"workflow",
				"tags":["stable"],
				"content":"# changed under the same key",
				"isAiGenerated":false,
				"sourceSessionIds":[]
			},
			"idempotencyKey":"conflict-key"
		}`
		conflict, status := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/revisions", conflictingAppend, owner)
		expectDSG46Error(conflict, status, http.StatusConflict,
			"revision_sequence_conflict", "The revision could not be appended because its identity conflicts.")

		By("reaching invalid_generation_state by canceling an already canceled generation")
		generationID := uuid.NewString()
		_, err = store.CreateGeneration(ctx, storage.CreateGenerationInput{
			ID: generationID, SkillID: identity.ID, CreatorSubject: owner,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Cancelable generation", Description: "Stable terminal-state fixture.", Type: "workflow",
				Tags: []string{}, Content: "# queued", SourceSessionIDs: []string{},
			},
			AuthorContext: "exercise cancellation", SelectedSessionIDs: []string{},
			EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
			EvaluationCriteria: json.RawMessage(`[{"id":"intent","kind":"content","description":"Match intent","weight":1}]`),
			CreatedAt:          time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		canceled, cancelStatus := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/generations/"+generationID+"/cancellations", "", owner)
		Expect(cancelStatus).To(Equal(http.StatusOK), "response: %#v", canceled)
		Expect(canceled).To(HaveKeyWithValue("status", "canceled"))
		terminal, terminalStatus := doJSON(srv, http.MethodPost,
			"/api/skills/"+identity.ID+"/generations/"+generationID+"/cancellations", "", owner)
		expectDSG46Error(terminal, terminalStatus, http.StatusConflict,
			"invalid_generation_state", "The generation cannot be changed from its current state.")

		Expect(coveredDSG46Codes).To(HaveLen(6),
			"the stable-envelope specification must exercise every code required by DSG-46")
		for _, code := range []string{
			"revision_not_found",
			"revision_private",
			"revision_not_public",
			"latest_revision_conflict",
			"invalid_generation_state",
			"revision_sequence_conflict",
		} {
			Expect(coveredDSG46Codes).To(HaveKey(code))
		}
	})
})

// effectiveFilterCaptureStore proves the HTTP layer threads deployment filters
// into both revision-backed storage queries. Its backing memory store cannot
// evaluate external views, so the adapter strips them only after recording the
// boundary contract.
type effectiveFilterCaptureStore struct {
	*storage.MemoryStore
	listOpts         *storage.EffectiveSkillListOpts
	countOpts        *storage.EffectiveSkillCountOpts
	sessionOpts      *storage.EffectiveSkillSessionListOpts
	listErr          error
	incrementErr     error
	attachmentView   string
	attachmentLabels map[string][]string
}

func (s *effectiveFilterCaptureStore) ProbeExternalView(context.Context, string) error { return nil }

func (s *effectiveFilterCaptureStore) IncrementSkillDownloads(ctx context.Context, skillID string) error {
	if s.incrementErr != nil {
		return s.incrementErr
	}
	return s.MemoryStore.IncrementSkillDownloads(ctx, skillID)
}

func (s *effectiveFilterCaptureStore) ListEffectiveSkills(ctx context.Context, opts storage.EffectiveSkillListOpts) ([]storage.EffectiveSkillRecord, error) {
	captured := opts
	s.listOpts = &captured
	if s.listErr != nil {
		return nil, s.listErr
	}
	external := opts.External
	opts.External = nil
	rows, err := s.MemoryStore.ListEffectiveSkills(ctx, opts)
	if err != nil || s.attachmentLabels == nil {
		return rows, err
	}
	return s.filterByFixtureAttachments(rows, external), nil
}

func (s *effectiveFilterCaptureStore) ListEffectiveSkillsBySession(ctx context.Context, opts storage.EffectiveSkillSessionListOpts) ([]storage.EffectiveSkillRecord, error) {
	captured := opts
	s.sessionOpts = &captured
	return s.MemoryStore.ListEffectiveSkillsBySession(ctx, opts)
}

func (s *effectiveFilterCaptureStore) CountEffectiveSkills(ctx context.Context, opts storage.EffectiveSkillCountOpts) (storage.SkillCounts, error) {
	captured := opts
	s.countOpts = &captured
	if s.attachmentLabels == nil {
		opts.External = nil
		return s.MemoryStore.CountEffectiveSkills(ctx, opts)
	}
	rows, err := s.MemoryStore.ListEffectiveSkills(ctx, storage.EffectiveSkillListOpts{
		SkillListOpts: storage.SkillListOpts{Query: opts.Query, Limit: 100},
		CallerSubject: opts.CallerSubject,
	})
	if err != nil {
		return storage.SkillCounts{}, err
	}
	rows = s.filterByFixtureAttachments(rows, opts.External)
	counts := storage.SkillCounts{Total: int64(len(rows))}
	for _, row := range rows {
		if row.CardRevision != nil && row.CardRevision.Revision.CreatorSubject == opts.Author {
			counts.Mine++
		}
	}
	return counts, nil
}

func (s *effectiveFilterCaptureStore) filterByFixtureAttachments(
	rows []storage.EffectiveSkillRecord,
	filters []storage.ExternalAttachmentFilter,
) []storage.EffectiveSkillRecord {
	filtered := make([]storage.EffectiveSkillRecord, 0, len(rows))
	for _, row := range rows {
		matches := true
		for _, filter := range filters {
			if filter.View != s.attachmentView || filter.TypeValue != "skill" ||
				!containsEveryString(s.attachmentLabels[row.Skill.ID], filter.Values) {
				matches = false
				break
			}
		}
		if matches {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func containsEveryString(have, want []string) bool {
	for _, wanted := range want {
		found := false
		for _, value := range have {
			if value == wanted {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

var _ storage.SkillReader = (*effectiveFilterCaptureStore)(nil)

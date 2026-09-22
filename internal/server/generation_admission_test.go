package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// admissionRecordingStore counts durable generation enqueues. The count is the
// assertion that matters: a refusal that still wrote the row would satisfy an
// HTTP status check and violate the contract.
type admissionRecordingStore struct {
	*storage.MemoryStore
	creates atomic.Int32
}

func (s *admissionRecordingStore) CreateGeneration(
	ctx context.Context, input storage.CreateGenerationInput,
) (*storage.SkillGenerationRecord, error) {
	s.creates.Add(1)
	return s.MemoryStore.CreateGeneration(ctx, input)
}

func newAdmissionStore() *admissionRecordingStore {
	store := &admissionRecordingStore{MemoryStore: storage.NewMemoryStore()}
	DeferCleanup(store.Close)
	return store
}

// newAdmissionServer builds a server whose deployment either admits new
// generations or does not. generation.enabled=true is the zero value, so the
// enabled server is byte-for-byte the configuration every other spec uses.
func newAdmissionServer(store storage.Store, generationEnabled bool) *server.Server {
	cfg := server.Config{}
	cfg.Generation.AdmissionClosed = !generationEnabled
	return server.New(cfg, store, nil, nil)
}

const (
	admissionOwner   = "admission-owner"
	admissionPrefix  = "/api/" + server.DefaultName
	admissionRequest = `{"baseRevisionId":null,"input":{"name":"Generated",` +
		`"description":"complete","type":"workflow","tags":[],"content":"# generated",` +
		`"isAiGenerated":true,"sourceSessionIds":[]},"authorContext":"admission",` +
		`"selectedSessionIds":[]}`
)

func admissionSkillPath(skillID string, suffix ...string) string {
	return admissionPrefix + "/" + skillID + strings.Join(suffix, "")
}

// seedAdmissionGeneration commits one nonterminal generation directly through
// the store, standing in for work admitted before admission closed.
func seedAdmissionGeneration(store storage.GenerationStore, skillID string) string {
	generationID := uuid.NewString()
	_, err := store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
		ID: generationID, SkillID: skillID, CreatorSubject: admissionOwner,
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Already admitted", Description: "queued before the switch flipped",
			Type: "workflow", Tags: []string{}, Content: "# queued", SourceSessionIDs: []string{},
		},
		AuthorContext: "admitted earlier", SelectedSessionIDs: []string{},
		EvaluatorProfile: "admission-profile", EvaluatorProfileVersion: "1",
		EvaluationCriteria: json.RawMessage(`[{"id":"admission"}]`),
		CreatedAt:          time.Now().UTC().Add(-time.Minute),
	})
	Expect(err).NotTo(HaveOccurred())
	return generationID
}

func generationCount(store storage.GenerationStore, skillID string) int {
	page, err := store.ListSkillGenerations(context.Background(), storage.SkillGenerationListOpts{
		CallerSubject: admissionOwner, SkillID: skillID, Limit: storage.MaxGenerationListLimit,
	})
	Expect(err).NotTo(HaveOccurred())
	return len(page.Generations)
}

var _ = Describe("generation admission", func() {
	It("refuses new generations with a stable code before anything is enqueued", func() {
		store := newAdmissionStore()
		skill, _ := seedPublicRevision(store.MemoryStore, "admission-closed", admissionOwner, nil, nil)
		service := newAdmissionServer(store, false)

		body, status := doJSON(service, http.MethodPost,
			admissionSkillPath(skill.ID, "/generations"), admissionRequest, admissionOwner)

		Expect(status).To(Equal(http.StatusForbidden))
		failure, ok := body["error"].(map[string]any)
		Expect(ok).To(BeTrue(), "the refusal must use the shared bounded error envelope")
		Expect(failure["code"]).To(Equal("generation_disabled"))
		Expect(failure["message"]).To(BeAssignableToTypeOf(""))
		Expect(strings.ToLower(failure["message"].(string))).NotTo(SatisfyAny(
			ContainSubstring("plan"), ContainSubstring("tier"), ContainSubstring("upgrade"),
			ContainSubstring("entitle"), ContainSubstring("billing"),
		), "the cassette is told whether to admit work, never why")

		By("proving nothing durable was written")
		Expect(store.creates.Load()).To(BeZero(), "the gate must precede the durable enqueue")
		Expect(generationCount(store, skill.ID)).To(BeZero())
		list, listStatus := doJSON(service, http.MethodGet,
			admissionSkillPath(skill.ID, "/generations"), "", admissionOwner)
		Expect(listStatus).To(Equal(http.StatusOK))
		Expect(list["generations"]).To(BeEmpty())
	})

	It("leaves the admitted path untouched when the deployment admits generations", func() {
		for _, enabled := range []bool{true, true} {
			store := newAdmissionStore()
			skill, _ := seedPublicRevision(store.MemoryStore, "admission-open", admissionOwner, nil, nil)
			service := newAdmissionServer(store, enabled)

			body, status := doJSON(service, http.MethodPost,
				admissionSkillPath(skill.ID, "/generations"), admissionRequest, admissionOwner)

			Expect(status).To(Equal(http.StatusAccepted))
			Expect(body["status"]).To(Equal(string(storage.GenerationStatusQueued)))
			Expect(body["skillId"]).To(Equal(skill.ID))
			Expect(store.creates.Load()).To(Equal(int32(1)))
			Expect(generationCount(store, skill.ID)).To(Equal(1))
		}
	})

	It("settles admission before request validation but after authentication", func() {
		store := newAdmissionStore()
		skill, _ := seedPublicRevision(store.MemoryStore, "admission-order", admissionOwner, nil, nil)
		service := newAdmissionServer(store, false)
		path := admissionSkillPath(skill.ID, "/generations")

		By("answering an unauthenticated caller with 401, never leaking the admission state")
		_, status := doJSON(service, http.MethodPost, path, admissionRequest, "")
		Expect(status).To(Equal(http.StatusUnauthorized))

		By("refusing every request shape identically once authenticated")
		for _, request := range []string{
			admissionRequest,
			`{`,
			`{"unknownField":true}`,
			`{"baseRevisionId":"not-a-uuid","input":{},"authorContext":"x","selectedSessionIds":[]}`,
			"",
		} {
			body, status := doJSON(service, http.MethodPost, path, request, admissionOwner)
			Expect(status).To(Equal(http.StatusForbidden), "request %q", request)
			Expect(body["error"].(map[string]any)["code"]).To(Equal("generation_disabled"), "request %q", request)
		}

		By("refusing an unknown skill the same way rather than disclosing its absence")
		body, status := doJSON(service, http.MethodPost,
			admissionSkillPath(uuid.NewString(), "/generations"), admissionRequest, admissionOwner)
		Expect(status).To(Equal(http.StatusForbidden))
		Expect(body["error"].(map[string]any)["code"]).To(Equal("generation_disabled"))
		Expect(store.creates.Load()).To(BeZero())
	})

	It("offers no alternate route that admits generation work while admission is closed", func() {
		store := newAdmissionStore()
		skill, revision := seedPublicRevision(store.MemoryStore, "admission-routes", admissionOwner, nil, nil)
		admitted := seedAdmissionGeneration(store, skill.ID)
		before := generationCount(store, skill.ID)
		Expect(before).To(Equal(1))
		store.creates.Store(0)
		service := newAdmissionServer(store, false)

		// Every mutating route the cassette publishes, plus the shapes a caller
		// might reach for hoping one of them still enqueues: the reserved
		// direct-ID collection, a retired /generate verb, and the sibling
		// cancellation collection.
		routes := []struct {
			method string
			path   string
			body   string
		}{
			{http.MethodPost, admissionPrefix, `{"slug":"admission-routes"}`},
			{http.MethodPost, admissionSkillPath(skill.ID, "/revisions"),
				`{"snapshot":{"name":"Manual","description":"authored","type":"workflow","tags":[],` +
					`"content":"# manual","isAiGenerated":false,"sourceSessionIds":[]},` +
					`"idempotencyKey":"admission-manual"}`},
			{http.MethodPut, admissionSkillPath(skill.ID, "/revisions/", revision.ID, "/visibility"), `{"isPublic":true}`},
			{http.MethodPut, admissionSkillPath(skill.ID, "/latest"), `{"revisionId":"` + revision.ID + `"}`},
			{http.MethodDelete, admissionSkillPath(skill.ID, "/latest"), ""},
			{http.MethodPost, admissionSkillPath(skill.ID, "/generations/", admitted, "/cancellations"), ""},
			{http.MethodPost, admissionSkillPath(skill.ID, "/generations"), admissionRequest},
			{http.MethodPost, admissionPrefix + "/generations", admissionRequest},
			{http.MethodPost, admissionPrefix + "/generations/" + uuid.NewString(), admissionRequest},
			{http.MethodPost, admissionPrefix + "/generate", admissionRequest},
			{http.MethodPost, admissionSkillPath(skill.ID, "/generations/", admitted), admissionRequest},
			{http.MethodPost, admissionSkillPath(skill.ID, "/generations/", admitted, "/resolutions"), admissionRequest},
			{http.MethodPut, admissionSkillPath(skill.ID, "/generations"), admissionRequest},
			{http.MethodPatch, admissionSkillPath(skill.ID, "/generations"), admissionRequest},
			{http.MethodPost, admissionSkillPath(skill.ID, "/generations/"), admissionRequest},
		}
		for _, route := range routes {
			_, status := doJSON(service, route.method, route.path, route.body, admissionOwner)
			Expect(status).NotTo(Equal(http.StatusAccepted), "%s %s admitted generation work", route.method, route.path)
		}

		Expect(store.creates.Load()).To(BeZero(),
			"no published route may reach the durable generation enqueue while admission is closed")
		Expect(generationCount(store, skill.ID)).To(Equal(before))
	})

	It("keeps browsing, manual authoring and generation reads working while admission is closed", func() {
		store := newAdmissionStore()
		skill, revision := seedPublicRevision(store.MemoryStore, "admission-reads", admissionOwner, nil, nil)
		admitted := seedAdmissionGeneration(store, skill.ID)
		service := newAdmissionServer(store, false)

		unaffected := []struct {
			method string
			path   string
			body   string
			status int
		}{
			{http.MethodGet, admissionPrefix, "", http.StatusOK},
			{http.MethodGet, admissionSkillPath(skill.ID), "", http.StatusOK},
			{http.MethodGet, admissionSkillPath(skill.ID, "/skill.md"), "", http.StatusOK},
			{http.MethodGet, admissionSkillPath(skill.ID, "/revisions"), "", http.StatusOK},
			{http.MethodGet, admissionSkillPath(skill.ID, "/revisions/", revision.ID), "", http.StatusOK},
			{http.MethodGet, admissionSkillPath(skill.ID, "/generations"), "", http.StatusOK},
			{http.MethodGet, admissionSkillPath(skill.ID, "/generations/", admitted), "", http.StatusOK},
			{http.MethodGet, admissionPrefix + "/generations/" + admitted, "", http.StatusOK},
			// Manual authoring is a separate capability and stays open.
			{http.MethodPost, admissionPrefix, `{"slug":"admission-reads-two"}`, http.StatusOK},
			{http.MethodPost, admissionSkillPath(skill.ID, "/revisions"),
				`{"snapshot":{"name":"Manual","description":"authored by hand","type":"workflow","tags":[],` +
					`"content":"# manual","isAiGenerated":false,"sourceSessionIds":[]},` +
					`"idempotencyKey":"admission-reads-manual"}`, http.StatusCreated},
			{http.MethodPut, admissionSkillPath(skill.ID, "/revisions/", revision.ID, "/visibility"),
				`{"isPublic":true}`, http.StatusOK},
			{http.MethodPut, admissionSkillPath(skill.ID, "/latest"),
				`{"revisionId":"` + revision.ID + `"}`, http.StatusOK},
			{http.MethodDelete, admissionSkillPath(skill.ID, "/latest"), "", http.StatusOK},
			// Cancelling already-admitted work is a read-side mutation, not
			// admission, and must keep working so a drain can be shortened.
			{http.MethodPost, admissionSkillPath(skill.ID, "/generations/", admitted, "/cancellations"),
				"", http.StatusOK},
		}
		for _, route := range unaffected {
			body, status := doJSON(service, route.method, route.path, route.body, admissionOwner)
			Expect(status).To(Equal(route.status), "%s %s: %v", route.method, route.path, body)
		}

		By("serving the same /openapi and /ping anchors regardless of admission")
		open := newAdmissionServer(newAdmissionStore(), true)
		_, status := doJSON(service, http.MethodGet, "/ping", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(openAPIBytes(service)).To(Equal(openAPIBytes(open)),
			"admission is deployment state, never part of the published contract document")
	})
})

func openAPIBytes(service *server.Server) string {
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder,
		httptest.NewRequest(http.MethodGet, "/openapi", nil))
	Expect(recorder.Code).To(Equal(http.StatusOK))
	return recorder.Body.String()
}

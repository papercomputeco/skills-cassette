package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	cassettemanifest "github.com/papercomputeco/tapes/pkg/cassette/manifest"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

func authoredManifestMap() map[string]any {
	data, err := os.ReadFile("../../cassette.toml")
	Expect(err).NotTo(HaveOccurred())
	var raw map[string]any
	_, err = toml.Decode(string(data), &raw)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func embeddedManifestMap() map[string]any {
	var document map[string]json.RawMessage
	Expect(json.Unmarshal(openAPIDocument(DefaultName), &document)).To(Succeed())
	raw, ok := document["x-tapes-cassette"]
	Expect(ok).To(BeTrue())
	var manifest map[string]any
	Expect(json.Unmarshal(raw, &manifest)).To(Succeed())
	return manifest
}

func parsedManifest(raw map[string]any) cassette.Manifest {
	encoded, err := json.Marshal(raw)
	Expect(err).NotTo(HaveOccurred())
	manifest, err := cassettemanifest.Parse(encoded)
	Expect(err).NotTo(HaveOccurred())
	return manifest
}

func manifestTableNames(raw map[string]any) []string {
	encoded, err := json.Marshal(raw["tables"])
	Expect(err).NotTo(HaveOccurred())
	var tables []struct {
		Name string `json:"name"`
	}
	Expect(json.Unmarshal(encoded, &tables)).To(Succeed())
	names := make([]string, len(tables))
	for index, table := range tables {
		names[index] = table.Name
	}
	return names
}

type publishedIntegerBound struct {
	Default int
	Minimum int
	Maximum int
}

func manifestGenerationIntegerBounds(raw map[string]any) map[string]publishedIntegerBound {
	encoded, err := json.Marshal(raw["config"])
	Expect(err).NotTo(HaveOccurred())
	var entries []struct {
		Key     string `json:"key"`
		Type    string `json:"type"`
		Default any    `json:"default"`
		Minimum any    `json:"min"`
		Maximum any    `json:"max"`
	}
	Expect(json.Unmarshal(encoded, &entries)).To(Succeed())
	bounds := make(map[string]publishedIntegerBound)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Key, "generation.") {
			continue
		}
		Expect(entry.Type).To(Equal("int"), entry.Key)
		Expect(entry.Default).To(BeAssignableToTypeOf(float64(0)), entry.Key)
		Expect(entry.Minimum).To(BeAssignableToTypeOf(float64(0)), entry.Key)
		Expect(entry.Maximum).To(BeAssignableToTypeOf(float64(0)), entry.Key)
		bounds[entry.Key] = publishedIntegerBound{
			Default: int(entry.Default.(float64)), Minimum: int(entry.Minimum.(float64)), Maximum: int(entry.Maximum.(float64)),
		}
	}
	return bounds
}

type handlerInvocation struct {
	response   *httptest.ResponseRecorder
	panicValue any
}

func invokeHandler(handler http.Handler, method, path, body, subject string) (result handlerInvocation) {
	result.response = httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if subject != "" {
		request.Header.Set(authSubjectHeader, subject)
	}
	func() {
		defer func() { result.panicValue = recover() }()
		handler.ServeHTTP(result.response, request)
	}()
	return result
}

var _ = Describe("unified revision manifest and route surface", func() {
	It("openapi_and_routes_remove_draft_publication_mutations", func() {
		prefix := "/api/" + DefaultName
		var document struct {
			Paths map[string]map[string]json.RawMessage `json:"paths"`
		}
		Expect(json.Unmarshal(openAPIDocument(DefaultName), &document)).To(Succeed())

		wantMethods := map[string][]string{
			prefix:                                                         {"get", "post"},
			prefix + "/{skillId}":                                          {"get"},
			prefix + "/{skillId}/skill.md":                                 {"get"},
			prefix + "/{skillId}/revisions":                                {"get", "post"},
			prefix + "/{skillId}/revisions/{revisionId}":                   {"get"},
			prefix + "/{skillId}/revisions/{revisionId}/visibility":        {"put"},
			prefix + "/{skillId}/latest":                                   {"delete", "put"},
			prefix + "/{skillId}/generations":                              {"get", "post"},
			prefix + "/{skillId}/generations/{generationId}":               {"get"},
			prefix + "/generations/{generationId}":                         {"get"},
			prefix + "/{skillId}/generations/{generationId}/cancellations": {"post"},
		}
		gotMethods := make(map[string][]string, len(document.Paths))
		operationIDs := make([]string, 0)
		for path, item := range document.Paths {
			Expect(path == prefix || strings.HasPrefix(path, prefix+"/")).To(BeTrue(),
				"path %q escapes the admitted local prefix", path)
			for method, raw := range item {
				if method == "parameters" {
					continue
				}
				gotMethods[path] = append(gotMethods[path], method)
				var operation struct {
					ID        string                     `json:"operationId"`
					Responses map[string]json.RawMessage `json:"responses"`
				}
				Expect(json.Unmarshal(raw, &operation)).To(Succeed(), "%s %s", method, path)
				Expect(operation.ID).NotTo(BeEmpty(), "%s %s", method, path)
				Expect(operation.Responses).NotTo(BeEmpty(), "%s %s", method, path)
				operationIDs = append(operationIDs, operation.ID)
			}
			sort.Strings(gotMethods[path])
		}
		Expect(gotMethods).To(Equal(wantMethods),
			"OpenAPI must contain exactly identity/effective, revision, visibility, latest, markdown, and generation routes")
		Expect(operationIDs).To(HaveLen(15))
		Expect(operationIDs).To(ConsistOf(
			"listEffectiveSkills", "resolveSkillIdentity", "getEffectiveSkill", "getSkillMarkdown",
			"listSkillRevisions", "appendSkillRevision", "getSkillRevision",
			"setSkillRevisionVisibility", "setSkillLatestRevision", "clearSkillLatestRevision",
			"listSkillGenerations", "createSkillGeneration", "getSkillGeneration",
			"getSkillGenerationById", "cancelSkillGeneration",
		))

		for path := range document.Paths {
			lowerPath := strings.ToLower(path)
			for _, removedSegment := range []string{
				"/drafts", "/checkpoints", "/publications", "/resolutions",
				"/versions", "/duplicate", "/tags", "/labels",
			} {
				Expect(lowerPath).NotTo(ContainSubstring(removedSegment),
					"removed OpenAPI path segment %q remains in %q", removedSegment, path)
			}
			Expect(lowerPath).NotTo(Equal(prefix + "/generate"))
		}
		serialized := strings.ToLower(string(openAPIDocument(DefaultName)))
		for _, removed := range []string{
			`"operationid": "deleteskill"`, `"operationid": "deleteskillrevision"`,
			`"awaiting_input"`,
		} {
			Expect(serialized).NotTo(ContainSubstring(removed), "removed OpenAPI surface %q remains", removed)
		}

		By("exercising the same exact method tree on the live ServeMux")
		store := storage.NewMemoryStore()
		DeferCleanup(store.Close)
		now := time.Now().UTC()
		skillRecord, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "route-tree", CreatorSubject: "route-owner", CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		revision, err := store.AppendRevision(context.Background(), storage.AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skillRecord.ID, CreatorSubject: "route-owner",
			Origin: storage.RevisionOriginManual,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Route tree", Description: "route fixture", Type: "workflow",
				Tags: []string{"route"}, Content: "# route", SourceSessionIDs: []string{},
			},
			IdempotencyKey: "route-seed", CreatedAt: now.Add(time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.SetRevisionVisibility(context.Background(), storage.SetRevisionVisibilityInput{
			SkillID: skillRecord.ID, RevisionID: revision.ID, CallerSubject: "route-owner",
			IsPublic: true, ChangedAt: now.Add(2 * time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		generationID := uuid.NewString()
		_, err = store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
			ID: generationID, SkillID: skillRecord.ID, BaseRevisionID: revision.ID,
			CreatorSubject: "route-owner",
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Route generation", Description: "route input", Type: "workflow",
				Tags: []string{}, Content: "# generation", SourceSessionIDs: []string{},
			},
			EvaluatorProfile: "route-profile", EvaluatorProfileVersion: "1",
			EvaluationCriteria: json.RawMessage(`[{"id":"route"}]`), CreatedAt: now.Add(3 * time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		serverInstance := New(Config{}, store, nil, nil)
		var handler http.Handler
		Expect(func() { handler = serverInstance.Handler() }).NotTo(Panic(),
			"constructing the complete live route tree must never panic")
		appendBody := `{"snapshot":{"name":"Appended","description":"complete","type":"workflow","tags":[],"content":"# appended","isAiGenerated":false,"sourceSessionIds":[]},"idempotencyKey":"route-http-append"}`
		generationBody := `{"baseRevisionId":null,"input":{"name":"Generated","description":"complete","type":"workflow","tags":[],"content":"# generated","isAiGenerated":true,"sourceSessionIds":[]},"authorContext":"route","selectedSessionIds":[]}`
		present := []struct {
			method string
			path   string
			body   string
			status int
		}{
			{http.MethodGet, prefix, "", http.StatusOK},
			{http.MethodPost, prefix, `{"slug":"route-tree"}`, http.StatusOK},
			{http.MethodGet, prefix + "/" + skillRecord.ID, "", http.StatusOK},
			{http.MethodGet, prefix + "/" + skillRecord.ID + "/skill.md", "", http.StatusOK},
			{http.MethodGet, prefix + "/" + skillRecord.ID + "/revisions", "", http.StatusOK},
			{http.MethodPost, prefix + "/" + skillRecord.ID + "/revisions", appendBody, http.StatusCreated},
			{http.MethodGet, prefix + "/" + skillRecord.ID + "/revisions/" + revision.ID, "", http.StatusOK},
			{http.MethodPut, prefix + "/" + skillRecord.ID + "/revisions/" + revision.ID + "/visibility", `{"isPublic":true}`, http.StatusOK},
			{http.MethodPut, prefix + "/" + skillRecord.ID + "/latest", fmt.Sprintf(`{"revisionId":%q}`, revision.ID), http.StatusOK},
			{http.MethodDelete, prefix + "/" + skillRecord.ID + "/latest", "", http.StatusOK},
			{http.MethodGet, prefix + "/" + skillRecord.ID + "/generations", "", http.StatusOK},
			{http.MethodPost, prefix + "/" + skillRecord.ID + "/generations", generationBody, http.StatusAccepted},
			{http.MethodGet, prefix + "/" + skillRecord.ID + "/generations/" + generationID, "", http.StatusOK},
			{http.MethodGet, prefix + "/generations/" + generationID, "", http.StatusOK},
			{http.MethodPost, prefix + "/" + skillRecord.ID + "/generations/" + generationID + "/cancellations", "", http.StatusOK},
		}
		for _, route := range present {
			result := invokeHandler(handler, route.method, route.path, route.body, "route-owner")
			Expect(result.panicValue).To(BeNil(), "%s %s panicked: %v", route.method, route.path, result.panicValue)
			Expect(result.response.Code).To(Equal(route.status), "%s %s response: %s",
				route.method, route.path, result.response.Body.String())
		}

		By("proving every predecessor draft/detail/revision/generation mutation is absent")
		removed := []struct {
			method string
			path   string
		}{
			// Predecessor mutable skill-detail and published-version methods.
			{http.MethodDelete, prefix + "/" + skillRecord.ID},
			{http.MethodGet, prefix + "/" + skillRecord.ID + "/versions"},
			{http.MethodPost, prefix + "/" + skillRecord.ID + "/duplicate"},
			{http.MethodPut, prefix + "/" + skillRecord.ID},
			{http.MethodPatch, prefix + "/" + skillRecord.ID},
			{http.MethodDelete, prefix + "/" + skillRecord.ID + "/revisions/" + revision.ID},
			{http.MethodPut, prefix + "/" + skillRecord.ID + "/revisions/" + revision.ID},
			{http.MethodPost, prefix + "/generate"},

			// Every method formerly registered below the draft collection,
			// including draft detail, checkpoint revisions, publication, and
			// the complete draft-scoped generation lifecycle.
			{http.MethodGet, prefix + "/drafts"},
			{http.MethodPost, prefix + "/drafts"},
			{http.MethodGet, prefix + "/drafts/draft-id"},
			{http.MethodPatch, prefix + "/drafts/draft-id"},
			{http.MethodDelete, prefix + "/drafts/draft-id"},
			{http.MethodGet, prefix + "/drafts/draft-id/revisions"},
			{http.MethodPost, prefix + "/drafts/draft-id/revisions"},
			{http.MethodGet, prefix + "/drafts/draft-id/revisions/1"},
			{http.MethodPost, prefix + "/drafts/draft-id/checkpoints"},
			{http.MethodPost, prefix + "/drafts/draft-id/publications"},
			{http.MethodGet, prefix + "/drafts/draft-id/generations"},
			{http.MethodPost, prefix + "/drafts/draft-id/generations"},
			{http.MethodGet, prefix + "/drafts/draft-id/generations/" + generationID},
			{http.MethodPost, prefix + "/drafts/draft-id/generations/" + generationID + "/resolutions"},
			{http.MethodPost, prefix + "/drafts/draft-id/generations/" + generationID + "/cancellations"},

			// Resolution was never valid on the retained skill-scoped
			// generation route, and must not survive the cutover there either.
			{http.MethodPost, prefix + "/" + skillRecord.ID + "/generations/" + generationID + "/resolutions"},
		}
		for _, route := range removed {
			result := invokeHandler(handler, route.method, route.path, `{}`, "route-owner")
			Expect(result.panicValue).To(BeNil(), "%s %s panicked instead of being rejected: %v",
				route.method, route.path, result.panicValue)
			Expect(result.response.Code).To(Or(Equal(http.StatusNotFound), Equal(http.StatusMethodNotAllowed)),
				"%s %s unexpectedly remains live: %s", route.method, route.path, result.response.Body.String())
		}
	})

	It("manifest_and_openapi_digest_match_with_revision_surface", func() {
		contracts := []cassette.ContractVersion{"v1"}
		authoredRaw := authoredManifestMap()
		embeddedRaw := embeddedManifestMap()
		authored := parsedManifest(authoredRaw)
		embedded := parsedManifest(embeddedRaw)
		Expect(authored.Validate(contracts)).To(Succeed())
		Expect(embedded.Validate(contracts)).To(Succeed())

		authoredDigest, err := authored.Digest()
		Expect(err).NotTo(HaveOccurred())
		embeddedDigest, err := embedded.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(embeddedDigest).To(Equal(authoredDigest),
			"cassette.toml and x-tapes-cassette must describe one canonical manifest")

		unifiedTables := []string{
			"skills",
			"skill_revisions",
			"skill_revision_visibility",
			"skill_generations",
			"generation_sessions",
			"generation_candidates",
			"candidate_evaluations",
			"generation_diagnostics",
		}
		Expect(manifestTableNames(authoredRaw)).To(Equal(unifiedTables))
		Expect(manifestTableNames(embeddedRaw)).To(Equal(unifiedTables))
		for _, predecessor := range []string{
			"skill_versions", "skill_drafts", "draft_sessions", "draft_revisions", "publications",
		} {
			Expect(manifestTableNames(authoredRaw)).NotTo(ContainElement(predecessor))
			Expect(manifestTableNames(embeddedRaw)).NotTo(ContainElement(predecessor))
		}

		encodedAuthored, err := json.Marshal(authoredRaw)
		Expect(err).NotTo(HaveOccurred())
		encodedEmbedded, err := json.Marshal(embeddedRaw)
		Expect(err).NotTo(HaveOccurred())
		Expect(encodedEmbedded).To(MatchJSON(encodedAuthored),
			"manifest fields beyond tables must not drift while their canonical digest happens to match")

		By("pinning every manifest-published generation bound and cap")
		expectedGenerationBounds := map[string]publishedIntegerBound{
			"generation.worker_concurrency":    {Default: 2, Minimum: 1, Maximum: 64},
			"generation.max_sessions":          {Default: 8, Minimum: 1, Maximum: 100},
			"generation.candidate_concurrency": {Default: 2, Minimum: 1, Maximum: 100},
			"generation.poll_interval_ms":      {Default: 250, Minimum: 10, Maximum: 60000},
			"generation.max_poll_interval_ms":  {Default: 5000, Minimum: 10, Maximum: 300000},
			"generation.lease_duration_ms":     {Default: 120000, Minimum: 1000, Maximum: 3600000},
			"generation.heartbeat_interval_ms": {Default: 30000, Minimum: 100, Maximum: 600000},
			"generation.processing_timeout_ms": {Default: 300000, Minimum: 1000, Maximum: 3600000},
			"generation.drain_timeout_ms":      {Default: 10000, Minimum: 1000, Maximum: 300000},
			"generation.retry_backoff_ms":      {Default: 1000, Minimum: 10, Maximum: 300000},
			"generation.max_retry_backoff_ms":  {Default: 30000, Minimum: 10, Maximum: 3600000},
			"generation.max_attempts":          {Default: 3, Minimum: 1, Maximum: 10},
			"generation.max_transcript_bytes":  {Default: 1048576, Minimum: 4096, Maximum: 1048576},
		}
		Expect(manifestGenerationIntegerBounds(authoredRaw)).To(Equal(expectedGenerationBounds))
		Expect(manifestGenerationIntegerBounds(embeddedRaw)).To(Equal(expectedGenerationBounds))

		By("pinning concrete request and page bounds in the served OpenAPI contract")
		var document map[string]any
		Expect(json.Unmarshal(openAPIDocument(DefaultName), &document)).To(Succeed())
		paths := document["paths"].(map[string]any)
		prefix := "/api/" + DefaultName
		operationAt := func(path, method string) map[string]any {
			return paths[path].(map[string]any)[method].(map[string]any)
		}
		requestSchemaAt := func(path, method string) map[string]any {
			requestBody := operationAt(path, method)["requestBody"].(map[string]any)
			Expect(requestBody).To(HaveKeyWithValue("required", true), "%s %s request body", method, path)
			content := requestBody["content"].(map[string]any)
			Expect(content).To(HaveLen(1), "%s %s request media types", method, path)
			Expect(content).To(HaveKey("application/json"), "%s %s request media types", method, path)
			return content["application/json"].(map[string]any)["schema"].(map[string]any)
		}
		responseSchemaAt := func(path, method, status string) map[string]any {
			responses := operationAt(path, method)["responses"].(map[string]any)
			content := responses[status].(map[string]any)["content"].(map[string]any)
			return content["application/json"].(map[string]any)["schema"].(map[string]any)
		}
		querySchemaAt := func(path, method, name string) map[string]any {
			parameters := operationAt(path, method)["parameters"].([]any)
			for _, raw := range parameters {
				parameter := raw.(map[string]any)
				if parameter["in"] == "query" && parameter["name"] == name {
					return parameter["schema"].(map[string]any)
				}
			}
			Fail(fmt.Sprintf("%s %s has no %s query parameter", method, path, name))
			return nil
		}
		propertiesOf := func(schema map[string]any) map[string]any {
			return schema["properties"].(map[string]any)
		}

		resolveRequest := requestSchemaAt(prefix, "post")
		Expect(resolveRequest).To(HaveKeyWithValue("additionalProperties", false))
		Expect(propertiesOf(resolveRequest)["slug"]).To(And(
			HaveKeyWithValue("minLength", float64(1)),
			HaveKeyWithValue("maxLength", float64(128)),
		))
		identityResponse := responseSchemaAt(prefix, "post", "200")
		Expect(identityResponse).To(HaveKeyWithValue("required",
			ConsistOf("id", "slug", "explicitLatestRevisionId", "createdAt")))
		Expect(propertiesOf(identityResponse)).To(And(
			HaveLen(4),
			HaveKey("id"),
			HaveKey("slug"),
			HaveKey("explicitLatestRevisionId"),
			HaveKey("createdAt"),
		))
		Expect(propertiesOf(identityResponse)).NotTo(HaveKey("updatedAt"))
		Expect(propertiesOf(identityResponse)["slug"]).To(HaveKeyWithValue("minLength", float64(1)))
		Expect(propertiesOf(identityResponse)["explicitLatestRevisionId"]).To(
			HaveKeyWithValue("type", ConsistOf("string", "null")))

		appendRequest := requestSchemaAt(prefix+"/{skillId}/revisions", "post")
		Expect(appendRequest).To(HaveKeyWithValue("additionalProperties", false))
		appendProperties := propertiesOf(appendRequest)
		Expect(appendProperties["idempotencyKey"]).To(And(
			HaveKeyWithValue("minLength", float64(1)),
			HaveKeyWithValue("maxLength", float64(256)),
		))
		Expect(appendProperties["changeNote"]).To(And(
			HaveKeyWithValue("type", ConsistOf("string", "null")),
			HaveKeyWithValue("maxLength", float64(1024)),
		))
		for _, field := range []string{"basedOnRevisionId", "sourceRevisionId"} {
			Expect(appendProperties[field]).To(HaveKeyWithValue("type", ConsistOf("string", "null")))
		}
		appendSnapshot := appendProperties["snapshot"].(map[string]any)
		Expect(appendSnapshot).To(HaveKeyWithValue("additionalProperties", false))
		appendSnapshotProperties := propertiesOf(appendSnapshot)
		Expect(appendSnapshotProperties["name"]).To(And(
			HaveKeyWithValue("minLength", float64(1)),
			HaveKeyWithValue("maxLength", float64(1024)),
		))
		Expect(appendSnapshotProperties["description"]).To(HaveKeyWithValue("maxLength", float64(1048576)))
		Expect(appendSnapshotProperties["content"]).To(HaveKeyWithValue("maxLength", float64(1048576)))
		Expect(appendSnapshotProperties["tags"]).To(And(
			HaveKeyWithValue("maxItems", float64(64)),
			HaveKeyWithValue("items", HaveKeyWithValue("maxLength", float64(256))),
		))
		Expect(appendSnapshotProperties["sourceSessionIds"]).To(And(
			HaveKeyWithValue("maxItems", float64(100)),
			HaveKeyWithValue("items", HaveKeyWithValue("maxLength", float64(256))),
		))

		visibilityRequest := requestSchemaAt(prefix+"/{skillId}/revisions/{revisionId}/visibility", "put")
		Expect(visibilityRequest).To(And(
			HaveKeyWithValue("additionalProperties", false),
			HaveKeyWithValue("required", ConsistOf("isPublic")),
		))
		visibilityResponse := responseSchemaAt(prefix+"/{skillId}/revisions/{revisionId}/visibility", "put", "200")
		Expect(visibilityResponse).To(And(
			HaveKeyWithValue("additionalProperties", false),
			HaveKeyWithValue("required", ConsistOf("revisionId", "isPublic", "changedAt")),
		))
		Expect(propertiesOf(visibilityResponse)).To(And(
			HaveLen(3), HaveKey("revisionId"), HaveKey("isPublic"), HaveKey("changedAt"),
		))
		Expect(propertiesOf(visibilityResponse)).NotTo(HaveKey("snapshot"),
			"a visibility mutation must never promise private revision content")
		latestRequest := requestSchemaAt(prefix+"/{skillId}/latest", "put")
		Expect(latestRequest).To(And(
			HaveKeyWithValue("additionalProperties", false),
			HaveKeyWithValue("required", ConsistOf("revisionId")),
		))

		generationRequest := requestSchemaAt(prefix+"/{skillId}/generations", "post")
		Expect(generationRequest).To(HaveKeyWithValue("additionalProperties", false))
		generationProperties := propertiesOf(generationRequest)
		Expect(generationProperties["baseRevisionId"]).To(
			HaveKeyWithValue("type", ConsistOf("string", "null")))
		Expect(generationProperties["authorContext"]).To(HaveKeyWithValue("maxLength", float64(32768)))
		Expect(generationProperties["selectedSessionIds"]).To(And(
			HaveKeyWithValue("maxItems", float64(100)),
			HaveKeyWithValue("uniqueItems", true),
			HaveKeyWithValue("items", HaveKeyWithValue("maxLength", float64(256))),
		))
		generationInputProperties := propertiesOf(generationProperties["input"].(map[string]any))
		Expect(generationInputProperties["content"]).To(HaveKeyWithValue("maxLength", float64(1048576)))
		Expect(generationInputProperties["tags"]).To(HaveKeyWithValue("maxItems", float64(64)))

		generationResponse := responseSchemaAt(prefix+"/{skillId}/generations", "post", "202")
		generationResponseProperties := propertiesOf(generationResponse)
		candidateItems := generationResponseProperties["candidates"].(map[string]any)["items"].(map[string]any)
		insights := propertiesOf(candidateItems)["insights"].(map[string]any)
		Expect(insights).To(HaveKeyWithValue("maxItems", float64(storage.MaxGenerationCandidateInsights)))
		Expect(insights).To(HaveKeyWithValue("x-max-json-bytes", float64(storage.MaxGenerationCandidateInsightsJSONBytes)))
		insightItem := insights["items"].(map[string]any)
		Expect(insightItem).To(And(
			HaveKeyWithValue("additionalProperties", false),
			HaveKeyWithValue("required", ConsistOf("kind", "summary", "evidence")),
		))
		for _, property := range propertiesOf(insightItem) {
			Expect(property).To(HaveKeyWithValue("maxLength", float64(2048)))
		}

		evaluationItems := generationResponseProperties["evaluations"].(map[string]any)["items"].(map[string]any)
		evaluationProperties := propertiesOf(evaluationItems)
		criterionResults := evaluationProperties["criterionResults"].(map[string]any)
		Expect(criterionResults).To(HaveKeyWithValue("maxItems", float64(100)))
		criterionItem := criterionResults["items"].(map[string]any)
		Expect(criterionItem).To(And(
			HaveKeyWithValue("additionalProperties", false),
			HaveKeyWithValue("required", ConsistOf("criterion_id", "weight", "passed", "rationale")),
		))
		Expect(propertiesOf(criterionItem)).To(And(
			HaveKey("criterion_id"), HaveKey("weight"), HaveKey("passed"), HaveKey("rationale"),
		))

		findings := evaluationProperties["findings"].(map[string]any)
		Expect(findings).To(HaveKeyWithValue("maxItems", float64(50)))
		findingItem := findings["items"].(map[string]any)
		Expect(findingItem).To(HaveKeyWithValue("additionalProperties", false))
		Expect(propertiesOf(findingItem)).To(And(
			HaveKey("rule_id"), HaveKey("severity"), HaveKey("message"), HaveKey("file"), HaveKey("line"),
		))

		strengths := evaluationProperties["strengths"].(map[string]any)
		Expect(strengths).To(HaveKeyWithValue("maxItems", float64(50)))
		Expect(strengths["items"]).To(HaveKeyWithValue("type", "string"))
		Expect(evaluationProperties).NotTo(HaveKey("panel"),
			"provider-specific evaluator panels are never part of private or public generation history")

		diagnostics := generationResponseProperties["diagnostics"].(map[string]any)
		Expect(diagnostics).To(HaveKeyWithValue("maxItems", float64(storage.MaxGenerationDiagnostics)))
		diagnosticItem := diagnostics["items"].(map[string]any)
		Expect(diagnosticItem["oneOf"]).To(HaveLen(len(storage.GenerationDiagnosticDefinitions())))
		diagnosticProperties := propertiesOf(diagnosticItem)
		Expect(diagnosticProperties["stage"]).To(HaveKey("enum"))
		Expect(diagnosticProperties["code"]).To(HaveKey("enum"))
		Expect(diagnosticProperties["message"]).To(HaveKey("enum"))

		pageContracts := []struct {
			path              string
			arrayProperty     string
			maximum, fallback float64
		}{
			{path: prefix, arrayProperty: "items", maximum: 100, fallback: 24},
			{path: prefix + "/{skillId}/revisions", arrayProperty: "items", maximum: 100, fallback: 24},
			{path: prefix + "/{skillId}/generations", arrayProperty: "generations", maximum: 100, fallback: 20},
		}
		for _, contract := range pageContracts {
			limitSchema := querySchemaAt(contract.path, "get", "limit")
			Expect(limitSchema).To(And(
				HaveKeyWithValue("minimum", float64(1)),
				HaveKeyWithValue("maximum", contract.maximum),
				HaveKeyWithValue("default", contract.fallback),
			), contract.path)
			pageSchema := responseSchemaAt(contract.path, "get", "200")
			Expect(propertiesOf(pageSchema)[contract.arrayProperty]).
				To(HaveKeyWithValue("maxItems", contract.maximum), contract.path)
		}

		errorSchema := responseSchemaAt(prefix+"/{skillId}/revisions", "post", "409")
		errorProperties := propertiesOf(propertiesOf(errorSchema)["error"].(map[string]any))
		Expect(errorProperties["code"]).To(HaveKeyWithValue("maxLength", float64(128)))
		Expect(errorProperties["message"]).To(HaveKeyWithValue("maxLength", float64(1024)))
		Expect(errorProperties["resourceId"]).To(HaveKeyWithValue("type", "string"),
			"omitempty resourceId is optional, not nullable")
	})
})

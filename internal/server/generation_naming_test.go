package server_test

import (
	"encoding/json"
	"net/http"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

var _ = Describe("unnamed generation seeds", func() {
	It("accepts an empty name so the model can name the skill after its goal", func() {
		store := storage.NewMemoryStore()
		DeferCleanup(store.Close)
		srv := newSkillsServer(store)
		creator := "naming-creator"

		identity, status := doJSON(srv, http.MethodPost, "/api/skills",
			marshalBody(map[string]any{"slug": "session-01a0de40"}), creator)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", identity)
		skillID := identity["id"].(string)

		for _, name := range []string{"", "   ", "\t\n"} {
			generation, status := doJSON(srv, http.MethodPost, "/api/skills/"+skillID+"/generations",
				marshalBody(map[string]any{
					"baseRevisionId": nil,
					"input": map[string]any{
						"name": name, "description": "", "type": "workflow", "tags": []string{},
						"content": "", "isAiGenerated": true, "sourceSessionIds": []string{},
					},
					"authorContext":      "This skill is about CLI UX.",
					"selectedSessionIds": []string{"01a0de40"},
				}), creator)
			Expect(status).To(Equal(http.StatusAccepted), "name %q response: %#v", name, generation)
			Expect(generation["input"].(map[string]any)["name"]).To(Equal(""))
		}

		overlong, status := doJSON(srv, http.MethodPost, "/api/skills/"+skillID+"/generations",
			marshalBody(map[string]any{
				"baseRevisionId": nil,
				"input": map[string]any{
					"name": strings.Repeat(" ", 1025), "description": "", "type": "workflow", "tags": []string{},
					"content": "", "isAiGenerated": true, "sourceSessionIds": []string{},
				},
				"authorContext":      "",
				"selectedSessionIds": []string{"01a0de40"},
			}), creator)
		Expect(status).To(Equal(http.StatusBadRequest), "an overlong blank name must still hit the bound: %#v", overlong)

		generations, status := doJSON(srv, http.MethodGet, "/api/skills/"+skillID+"/generations", "", creator)
		Expect(status).To(Equal(http.StatusOK), "response: %#v", generations)
		Expect(generations["generations"]).To(HaveLen(3))
	})
})

func marshalBody(value any) string {
	body, err := json.Marshal(value)
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

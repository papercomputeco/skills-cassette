package server

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	cassettemanifest "github.com/papercomputeco/tapes/pkg/cassette/manifest"
)

// The manifest is published in two encodings — cassette.toml for whoever
// starts the process, and x-tapes-cassette in the OpenAPI document for core.
// They are one schema, and these specs hold them to it: both must validate
// against the v1 contract and canonicalize to the same digest. This file
// lives in package server (not server_test) because openAPIDocument is
// unexported; parity only holds for the default installation identity, so
// the embedded copy is built with DefaultName.
var _ = Describe("cassette manifest", func() {
	contracts := []cassette.ContractVersion{"v1"}

	tomlManifestMap := func() map[string]any {
		data, err := os.ReadFile("../../cassette.toml")
		Expect(err).NotTo(HaveOccurred())
		var raw map[string]any
		_, err = toml.Decode(string(data), &raw)
		Expect(err).NotTo(HaveOccurred())
		return raw
	}

	embeddedManifestMap := func() map[string]any {
		var doc map[string]json.RawMessage
		Expect(json.Unmarshal(openAPIDocument(DefaultName), &doc)).To(Succeed())
		rawExtension, ok := doc["x-tapes-cassette"]
		Expect(ok).To(BeTrue(), "OpenAPI document must carry the x-tapes-cassette extension")
		var raw map[string]any
		Expect(json.Unmarshal(rawExtension, &raw)).To(Succeed())
		return raw
	}

	parseManifest := func(raw map[string]any) cassette.Manifest {
		encoded, err := json.Marshal(raw)
		Expect(err).NotTo(HaveOccurred())
		parsed, err := cassettemanifest.Parse(encoded)
		Expect(err).NotTo(HaveOccurred())
		return parsed
	}

	loadTOML := func() cassette.Manifest {
		return parseManifest(tomlManifestMap())
	}

	loadEmbedded := func() cassette.Manifest {
		return parseManifest(embeddedManifestMap())
	}

	It("validates the authored TOML against the v1 contract", func() {
		Expect(loadTOML().Validate(contracts)).To(Succeed())
	})

	It("validates the embedded OpenAPI copy against the v1 contract", func() {
		Expect(loadEmbedded().Validate(contracts)).To(Succeed())
	})

	It("produces the same canonical digest from both encodings", func() {
		tomlDigest, err := loadTOML().Digest()
		Expect(err).NotTo(HaveOccurred())

		embeddedDigest, err := loadEmbedded().Digest()
		Expect(err).NotTo(HaveOccurred())

		Expect(embeddedDigest).To(Equal(tomlDigest),
			"cassette.toml and the manifest embedded in openapi.go have drifted apart")
	})

	It("advertises the skill entity in its manifest", func() {
		// Pure self-description (the runtime entity registry's declaration
		// side): the cassette states what it offers and knows nothing about
		// any consumer. Both encodings must carry the identical declaration.
		normalize := func(value any) []map[string]any {
			encoded, err := json.Marshal(value)
			Expect(err).NotTo(HaveOccurred())
			var entities []map[string]any
			Expect(json.Unmarshal(encoded, &entities)).To(Succeed())
			return entities
		}

		declared := normalize(tomlManifestMap()["entities"])
		embedded := normalize(embeddedManifestMap()["entities"])

		Expect(declared).To(Equal(embedded),
			"cassette.toml and the embedded manifest disagree on entity declarations")
		Expect(declared).To(HaveLen(1))
		Expect(declared[0]).To(Equal(map[string]any{
			"type":         "skill",
			"id_kind":      "uuid",
			"display_name": "Skill",
		}))
	})

	It("keeps every documented path under the declared prefix", func() {
		var doc struct {
			Paths map[string]json.RawMessage `json:"paths"`
		}
		Expect(json.Unmarshal(openAPIDocument(DefaultName), &doc)).To(Succeed())
		Expect(doc.Paths).NotTo(BeEmpty())
		for path := range doc.Paths {
			Expect(path == "/api/"+DefaultName || strings.HasPrefix(path, "/api/"+DefaultName+"/")).To(BeTrue(),
				"path %q escapes the cassette prefix", path)
		}
	})
})

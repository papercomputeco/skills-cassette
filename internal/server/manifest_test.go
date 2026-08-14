package server

import (
	"encoding/json"
	"strings"

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

	loadTOML := func() cassette.Manifest {
		declared, err := cassettemanifest.Load("../../cassette.toml")
		Expect(err).NotTo(HaveOccurred())
		return declared
	}

	loadEmbedded := func() cassette.Manifest {
		var doc map[string]json.RawMessage
		Expect(json.Unmarshal(openAPIDocument(DefaultName), &doc)).To(Succeed())
		raw, ok := doc["x-tapes-cassette"]
		Expect(ok).To(BeTrue(), "OpenAPI document must carry the x-tapes-cassette extension")
		embedded, err := cassettemanifest.Parse(raw)
		Expect(err).NotTo(HaveOccurred())
		return embedded
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

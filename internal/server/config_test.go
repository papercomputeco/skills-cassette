package server_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
)

var _ = Describe("core url validation", func() {
	It("allows an empty value: generation is simply disabled", func() {
		Expect(server.ValidateCoreURL("")).To(Succeed())
	})

	It("allows plaintext http only for loopback targets", func() {
		Expect(server.ValidateCoreURL("http://127.0.0.1:8081")).To(Succeed())
		Expect(server.ValidateCoreURL("http://localhost:8081")).To(Succeed())
		Expect(server.ValidateCoreURL("http://[::1]:8081")).To(Succeed())
	})

	It("requires https for non-loopback targets", func() {
		Expect(server.ValidateCoreURL("http://tapes.internal:8081")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("https://tapes.internal:8081")).To(Succeed())
	})

	It("allows plaintext http for cluster-local Service names", func() {
		Expect(server.ValidateCoreURL("http://gw-api.tenant-abc123.svc.cluster.local:8091")).To(Succeed())
		Expect(server.ValidateCoreURL("http://gw-api.tenant-abc123.svc:8091")).To(Succeed())
		Expect(server.ValidateCoreURL("http://gw-api.tenant-abc123.svc.cluster.local.:8091")).To(Succeed())
	})

	It("does not mistake lookalike hosts for cluster-local names", func() {
		Expect(server.ValidateCoreURL("http://evil-svc.example.com:8091")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("http://svc.cluster.local.example.com:8091")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("http://cluster.local:8091")).NotTo(Succeed())
	})

	It("refuses urls that are not clean http(s) origins", func() {
		Expect(server.ValidateCoreURL("ftp://tapes.internal")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("https://user:pass@tapes.internal")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("https://tapes.internal/api?x=1")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("https://tapes.internal#frag")).NotTo(Succeed())
		Expect(server.ValidateCoreURL("https://")).NotTo(Succeed())
	})
})

var _ = Describe("config from env", func() {
	It("reads the manifest-declared CASSETTE_* settings", func() {
		GinkgoT().Setenv("CASSETTE_NAME", "skills-two")
		GinkgoT().Setenv("CASSETTE_CORE_URL", "https://tapes.internal")
		GinkgoT().Setenv("CASSETTE_LLM_PROVIDER", "anthropic")
		GinkgoT().Setenv("CASSETTE_LLM_MODEL", "claude-haiku-4-5-20251001")
		GinkgoT().Setenv("CASSETTE_LLM_API_KEY", "sk-test")
		GinkgoT().Setenv("CASSETTE_LLM_BASE_URL", "https://proxy.internal")

		cfg := server.ConfigFromEnv()
		Expect(cfg.Name).To(Equal("skills-two"))
		Expect(cfg.CoreURL).To(Equal("https://tapes.internal"))
		Expect(cfg.LLM.Provider).To(Equal("anthropic"))
		Expect(cfg.LLM.Model).To(Equal("claude-haiku-4-5-20251001"))
		Expect(cfg.LLM.APIKey).To(Equal("sk-test"))
		Expect(cfg.LLM.BaseURL).To(Equal("https://proxy.internal"))
	})

	It("defaults the name when the environment is empty", func() {
		GinkgoT().Setenv("CASSETTE_NAME", "")
		Expect(server.ConfigFromEnv().Name).To(Equal(server.DefaultName))
	})
})

var _ = Describe("external filter configuration", func() {
	It("parses a deployment-supplied filter list", func() {
		filters, err := server.ParseExternalFilters(
			`[{"param":"label","view":"attach_fixture.attachments","type_value":"skill","normalize":["trim","nfc","casefold"]}]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(filters).To(HaveLen(1))
		Expect(filters[0].Param).To(Equal("label"))
		Expect(filters[0].View).To(Equal("attach_fixture.attachments"))
		Expect(filters[0].TypeValue).To(Equal("skill"))
		Expect(filters[0].Normalize).To(Equal([]string{"trim", "nfc", "casefold"}))
	})

	It("treats an absent value as the capability being off", func() {
		filters, err := server.ParseExternalFilters("")
		Expect(err).NotTo(HaveOccurred())
		Expect(filters).To(BeEmpty())

		filters, err = server.ParseExternalFilters("   ")
		Expect(err).NotTo(HaveOccurred())
		Expect(filters).To(BeEmpty())
	})

	It("refuses malformed filter entries at startup", func() {
		malformed := []string{
			`not json`,
			`[{"param":"label"}]`, // missing view and type_value
			`[{"param":"label","view":"a.b","type_value":"skill","unknown":"x"}]`,                               // unknown field
			`[{"param":"Bad-Param","view":"a.b","type_value":"skill"}]`,                                         // param grammar
			`[{"param":"cursor","view":"a.b","type_value":"skill"}]`,                                            // reserved param
			`[{"param":"p","view":"unqualified","type_value":"skill"}]`,                                         // view must be schema-qualified
			`[{"param":"p","view":"a.b.c","type_value":"skill"}]`,                                               // too many segments
			`[{"param":"p","view":"A.b","type_value":"skill"}]`,                                                 // uppercase view
			`[{"param":"p","view":"a.b","type_value":"Not A Token"}]`,                                           // type_value grammar
			`[{"param":"p","view":"a.b","type_value":"skill","normalize":["upper"]}]`,                           // unknown verb
			`[{"param":"p","view":"a.b","type_value":"skill"},{"param":"p","view":"c.d","type_value":"skill"}]`, // duplicate param
		}
		for _, raw := range malformed {
			_, err := server.ParseExternalFilters(raw)
			Expect(err).To(HaveOccurred(), "expected %s to be refused", raw)
		}
	})

	It("refuses an overlong view segment", func() {
		segment := strings.Repeat("a", 64)
		_, err := server.ParseExternalFilters(
			`[{"param":"p","view":"` + segment + `.b","type_value":"skill"}]`)
		Expect(err).To(HaveOccurred())
	})

	It("applies the configured normalize verbs in order", func() {
		Expect(server.NormalizeFilterValue("  Hello  ", []string{"trim"})).To(Equal("Hello"))
		Expect(server.NormalizeFilterValue("HeLLo", []string{"casefold"})).To(Equal("hello"))
		// NFC composes a combining acute accent onto its base letter.
		Expect(server.NormalizeFilterValue("é", []string{"nfc"})).To(Equal("é"))
		// Full casefold expands sharp s.
		Expect(server.NormalizeFilterValue("  Straße ", []string{"trim", "nfc", "casefold"})).To(Equal("strasse"))
		// No verbs declared: the value binds raw.
		Expect(server.NormalizeFilterValue("  RaW ", nil)).To(Equal("  RaW "))
	})
})

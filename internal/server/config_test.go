package server_test

import (
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

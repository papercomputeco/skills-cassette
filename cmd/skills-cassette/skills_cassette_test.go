package skillscassettecmder_test

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	skillscassettecmder "github.com/papercomputeco/skills-cassette/cmd/skills-cassette"
)

var _ = Describe("skills-cassette command", func() {
	It("has a concise help and version surface", func() {
		var output bytes.Buffer
		cmd := skillscassettecmder.NewSkillsCassetteCmd()
		cmd.SetOut(&output)
		cmd.SetArgs([]string{"--help"})
		Expect(cmd.Execute()).To(Succeed())
		Expect(output.String()).To(ContainSubstring("skills-cassette"))
		Expect(output.String()).To(ContainSubstring("serve"))

		output.Reset()
		cmd = skillscassettecmder.NewSkillsCassetteCmd()
		cmd.SetOut(&output)
		cmd.SetArgs([]string{"--version"})
		Expect(cmd.Execute()).To(Succeed())
		Expect(output.String()).To(Equal("skills-cassette version dev\n"))
	})

	It("uses the release-injected version value", func() {
		original := skillscassettecmder.Version
		skillscassettecmder.Version = "v1.2.3"
		DeferCleanup(func() { skillscassettecmder.Version = original })

		var output bytes.Buffer
		cmd := skillscassettecmder.NewSkillsCassetteCmd()
		cmd.SetOut(&output)
		cmd.SetArgs([]string{"--version"})
		Expect(cmd.Execute()).To(Succeed())
		Expect(output.String()).To(Equal("skills-cassette version v1.2.3\n"))
	})

	It("rejects positional root arguments", func() {
		cmd := skillscassettecmder.NewSkillsCassetteCmd()
		cmd.SetArgs([]string{"unexpected"})
		Expect(cmd.Execute()).NotTo(Succeed())
	})
})

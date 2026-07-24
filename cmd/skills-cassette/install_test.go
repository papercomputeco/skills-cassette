package skillscassettecmder_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("installer", func() {
	It("rejects a binary whose published checksum does not match", func() {
		if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
			Skip("installer supports Linux and macOS")
		}

		root := GinkgoT().TempDir()
		artifactDir := filepath.Join(root, "skills-cassette", "v-test", runtime.GOOS, runtime.GOARCH)
		Expect(os.MkdirAll(artifactDir, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(artifactDir, "skills-cassette"), []byte("not the published binary"), 0o755)).To(Succeed())
		Expect(os.WriteFile(
			filepath.Join(artifactDir, "skills-cassette.sha256"),
			[]byte("0000000000000000000000000000000000000000000000000000000000000000  skills-cassette\n"),
			0o600,
		)).To(Succeed())

		installDir := filepath.Join(root, "install")
		Expect(os.MkdirAll(installDir, 0o755)).To(Succeed())
		cmd := exec.Command("bash", filepath.Join("..", "..", "install.sh"))
		cmd.Env = append(os.Environ(),
			"SKILLS_CASSETTE_VERSION=v-test",
			"SKILLS_CASSETTE_BASE_URL=file://"+root,
			"SKILLS_CASSETTE_INSTALL_DIR="+installDir,
		)
		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred(), string(output))
		Expect(filepath.Join(installDir, "skills-cassette")).NotTo(BeAnExistingFile())
	})
})

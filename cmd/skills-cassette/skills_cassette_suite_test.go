package skillscassettecmder_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSkillsCassette(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Skills Cassette Command Suite")
}

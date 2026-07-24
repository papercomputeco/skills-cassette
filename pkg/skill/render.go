package skill

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func RenderSkillMD(sk *Skill) string {
	var b strings.Builder

	b.WriteString("---\n")
	fmt.Fprintf(&b, "name: %s\n", frontmatterJSON(sk.Name))
	fmt.Fprintf(&b, "description: %s\n", frontmatterJSON(sk.Description))
	fmt.Fprintf(&b, "version: %s\n", frontmatterJSON(sk.Version))
	if len(sk.Tags) > 0 {
		fmt.Fprintf(&b, "tags: %s\n", frontmatterJSON(sk.Tags))
	}
	if sk.Type != "" {
		fmt.Fprintf(&b, "type: %s\n", frontmatterJSON(sk.Type))
	}
	if len(sk.Sessions) > 0 {
		fmt.Fprintf(&b, "sessions: %s\n", frontmatterJSON(sk.Sessions))
	}
	if !sk.CreatedAt.IsZero() {
		fmt.Fprintf(&b, "created_at: %s\n", frontmatterJSON(sk.CreatedAt.Format(time.RFC3339)))
	}
	b.WriteString("---\n\n")
	b.WriteString(sk.Content)

	// Ensure trailing newline
	if !strings.HasSuffix(sk.Content, "\n") {
		b.WriteString("\n")
	}

	return b.String()
}

// frontmatterJSON emits JSON scalars and arrays, which are valid YAML 1.2.
// Quoting prevents generated text from terminating or reshaping frontmatter.
func frontmatterJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("encode SKILL.md frontmatter: %v", err))
	}
	return string(encoded)
}

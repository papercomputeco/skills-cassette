package server

import (
	"encoding/json"
	"strings"
)

// Version is the cassette release identity published in the manifest and the
// OpenAPI info block. Keep it in sync with cassette.toml.
const Version = "0.4.0"

// openAPIDocument renders this cassette's OpenAPI document.
//
// Every path is written under /api/<name>, which is what core's prefix
// admission requires: a fetched spec that declares an operation outside its
// own prefix is refused whole. Building the document from the runtime name
// rather than hardcoding "skills" means the same image installed under a
// second name publishes a correct spec for that name too.
//
// The manifest core admits the cassette on rides inside the document as the
// x-tapes-cassette root extension, so there is one artifact to fetch and one
// thing to configure — and so a spec and the metadata describing it can never
// be fetched at two different versions. cassette.toml is the authored twin of
// the extension below; the two must stay in sync.
func openAPIDocument(name string) []byte {
	prefix := "/api/" + name

	document := map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":       "Skills cassette",
			"description": "Generates, stores, versions, and serves reusable SKILL.md skills extracted from Tapes sessions.",
			"version":     Version,
		},
		"x-tapes-cassette": manifest(name),
		"paths": map[string]any{
			prefix: map[string]any{
				"get": operation("listSkills", "List skills",
					"One keyset page of skills, newest-edited first, plus per-tab counts for the "+
						"active search. Pagination mirrors /v1/sessions: pass the returned next_cursor "+
						"to continue; its absence means the last page.\n\nPassing session_id switches "+
						"the route to the provenance reverse lookup: the skills generated from that "+
						"session, unpaginated, in the legacy GET /v1/sessions/:id/skills envelope.",
					name,
					withParameters(
						queryParam("limit", "integer", "Page size (default 24, max 100)"),
						queryParam("cursor", "string", "Opaque keyset cursor from a previous next_cursor. Reset it when changing sort."),
						queryParam("q", "string", "Search over name, description, and tags"),
						queryParam("scope", "string", "Which attribution slice to return: all, mine, or team"),
						queryParam("status", "string", "Lifecycle filter: draft or published"),
						queryParam("sort", "string", "Ordering; \"downloads\" for most-downloaded, defaults to most recently updated"),
						queryParam("session_id", "string", "Return only the skills generated from this session (unpaginated)"),
					),
					withResponses(
						jsonResponse("200", "One page of skills", skillsListSchema()),
						jsonResponse("400", "Malformed cursor", errorSchema()),
						jsonResponse("500", "Listing failed", errorSchema()),
					)),
				"post": operation("createSkill", "Create a skill draft",
					"Persists an authored or AI-generated working draft. Generation itself is "+
						"side-effect-free; the caller supplies the chosen content and provenance.",
					name,
					withRequestBody("Skill to create", createSkillSchema()),
					withResponses(
						jsonResponse("201", "The created skill", skillSchema()),
						jsonResponse("400", "Invalid body or unknown type", errorSchema()),
						jsonResponse("500", "Create failed", errorSchema()),
					)),
			},
			prefix + "/generate": map[string]any{
				"post": operation("generateSkill", "Generate an ephemeral skill draft",
					"Runs the LLM generator over an author brief, nominated sessions, or both, "+
						"then returns an unpersisted draft. Ordered feedback is replayed on each "+
						"regeneration. Source transcripts are read from Tapes core over its public "+
						"trace API; the cassette holds no core database credential.",
					name,
					withRequestBody("Sources, brief, and cumulative feedback", generateSkillSchema()),
					withResponses(
						jsonResponse("200", "The generated, unpersisted draft", generatedSkillSchema()),
						jsonResponse("400", "Invalid or oversized input", errorSchema()),
						jsonResponse("404", "One or more source sessions were not found", errorSchema()),
						jsonResponse("422", "Sources carried nothing usable, or no LLM key is configured", errorSchema()),
						jsonResponse("500", "Generation failed", errorSchema()),
						jsonResponse("501", "Session generation requested without a core url", errorSchema()),
					)),
			},
			prefix + "/revise": map[string]any{
				"post": operation("reviseSkill", "Revise an ephemeral skill document",
					"Rewrites the supplied working markdown according to one instruction and "+
						"returns the replacement content without persisting it.",
					name,
					withRequestBody("Working content and revision instruction", reviseSkillSchema()),
					withResponses(
						jsonResponse("200", "The revised content", revisedSkillSchema()),
						jsonResponse("400", "Invalid or oversized input", errorSchema()),
						jsonResponse("422", "No LLM key is configured", errorSchema()),
						jsonResponse("500", "Revision failed", errorSchema()),
					)),
			},
			prefix + "/{id}": map[string]any{
				"parameters": []any{idPathParam("Skill id")},
				"get": operation("getSkill", "Get a skill",
					"Returns one skill by its opaque id. The id is the route key; slug is a "+
						"cosmetic display label and is not addressable.",
					name,
					withResponses(
						jsonResponse("200", "The skill", skillSchema()),
						jsonResponse("404", "Skill not found", errorSchema()),
						jsonResponse("500", "Lookup failed", errorSchema()),
					)),
				"put": operation("updateSkill", "Update a skill",
					"Partial update of the skill head. Every field is optional; omitted fields are "+
						"left as they are. Editing the head does not publish — use the versions "+
						"endpoint to snapshot.",
					name,
					withRequestBody("Fields to change", updateSkillSchema()),
					withResponses(
						jsonResponse("200", "The updated skill", skillSchema()),
						jsonResponse("400", "Invalid body or unknown type", errorSchema()),
						jsonResponse("404", "Skill not found", errorSchema()),
						jsonResponse("500", "Save failed", errorSchema()),
					)),
				"delete": operation("deleteSkill", "Delete a skill",
					"Deletes the skill and its version history. Only the creator may delete; "+
						"another caller gets 403 rather than 404, so the skill's existence is not "+
						"hidden from someone who can already list it.",
					name,
					withResponses(
						emptyResponse("204", "Deleted"),
						jsonResponse("403", "Only the creator can delete this skill", errorSchema()),
						jsonResponse("404", "Skill not found", errorSchema()),
						jsonResponse("500", "Delete failed", errorSchema()),
					)),
			},
			prefix + "/{id}/skill.md": map[string]any{
				"parameters": []any{idPathParam("Skill id")},
				"get": operation("getSkillMarkdown", "Download a skill as SKILL.md",
					"Renders the skill as an on-disk SKILL.md, served as an attachment. The "+
						"frontmatter name is the kebab slug, which is what a harness matches to the "+
						"skill's directory — not the human display name.\n\nServing this counts a "+
						"download, best-effort: a failed counter write never fails the download.",
					name,
					withResponses(
						contentResponse("200", "SKILL.md document", "text/markdown", map[string]any{"type": "string"}),
						jsonResponse("404", "Skill not found", errorSchema()),
						jsonResponse("500", "Lookup failed", errorSchema()),
					)),
			},
			prefix + "/{id}/versions": map[string]any{
				"parameters": []any{idPathParam("Skill id")},
				"get": operation("listSkillVersions", "List a skill's versions",
					"Full published history for one skill, newest first. Returned whole rather "+
						"than paged, so totalCount is always the length of versions.",
					name,
					withResponses(
						jsonResponse("200", "The skill's versions", skillVersionsSchema()),
						jsonResponse("500", "Listing failed", errorSchema()),
					)),
				"post": operation("publishSkill", "Publish a skill version",
					"In one transaction, snapshots the current content as an immutable version, "+
						"advances the skill head, marks its lifecycle published, and widens visibility "+
						"to team. Versions are history; current content remains on the skill row.",
					name,
					withRequestBody("Version metadata", publishSkillSchema()),
					withResponses(
						jsonResponse("201", "The published version", skillVersionSchema()),
						jsonResponse("404", "Skill not found", errorSchema()),
						jsonResponse("500", "Publish failed, or the version landed but the head could not be advanced", errorSchema()),
					)),
			},
			prefix + "/{id}/duplicate": map[string]any{
				"parameters": []any{idPathParam("Skill id to duplicate")},
				"post": operation("duplicateSkill", "Duplicate a skill",
					"Forks a skill into a new one owned by the caller, with parentId set to the "+
						"source. The copy starts its own version history; the source is untouched.",
					name,
					withResponses(
						jsonResponse("201", "The duplicated skill", skillSchema()),
						jsonResponse("404", "Skill not found", errorSchema()),
						jsonResponse("500", "Duplicate failed", errorSchema()),
					)),
			},
		},
	}

	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		// Every value above is a literal that cannot fail to marshal — an error
		// means this function is wrong, not that the request is. Serving an
		// empty body would hide that; core reporting a cassette whose document
		// does not parse is the louder and more useful failure.
		return []byte(`{"error":"could not compile this cassette's OpenAPI document: ` +
			strings.ReplaceAll(err.Error(), `"`, `'`) + `"}`)
	}
	return encoded
}

// manifest is the metadata core admits this cassette on: the JSON encoding of
// the cassette/v1alpha1 schema whose authored twin is cassette.toml.
func manifest(name string) map[string]any {
	return map[string]any{
		"kind": "cassette/v1alpha1",
		"cassette": map[string]any{
			"name":         name,
			"version":      Version,
			"display_name": "Skills",
			"description":  "Generates, stores, versions, and serves reusable SKILL.md skills extracted from Tapes sessions.",
			"license":      "MIT OR Apache-2.0",
			"homepage":     "https://github.com/papercomputeco/skills-cassette",
			"image":        "public.ecr.aws/g4e5l3z3/papercomputeco/skills-cassette:v" + Version,
			"port":         9998,
		},
		"depends": map[string]any{
			"core":  "v1",
			"views": []string{},
		},
		"api": map[string]any{
			"health":      "/ping",
			"openapi":     "/openapi",
			"prefix_path": "api",
		},
		"tables": []map[string]any{
			{"name": "skills"},
			{"name": "skill_versions"},
		},
		"config": []map[string]any{
			{
				"key":         "core.url",
				"type":        "string",
				"required":    true,
				"description": "Tapes core API origin the generator reads trace transcripts from.",
			},
			{
				"key":         "llm.provider",
				"type":        "string",
				"default":     "openai",
				"enum":        []string{"openai", "anthropic", "ollama"},
				"description": "LLM provider used to extract skills.",
			},
			{
				"key":         "llm.model",
				"type":        "string",
				"description": "Model override; each provider has a sensible default.",
			},
			{
				"key":         "llm.api_key",
				"type":        "string",
				"secret":      true,
				"description": "Provider API key. Not required for ollama.",
			},
			{
				"key":         "llm.base_url",
				"type":        "string",
				"description": "Provider base URL override for proxies and self-hosted endpoints.",
			},
		},
	}
}

// --- OpenAPI assembly helpers ---

type operationOption func(map[string]any)

func operation(id, summary, description, tag string, opts ...operationOption) map[string]any {
	op := map[string]any{
		"operationId": id,
		"summary":     summary,
		"description": description,
		"tags":        []string{tag},
	}
	for _, opt := range opts {
		opt(op)
	}
	return op
}

func withParameters(params ...any) operationOption {
	return func(op map[string]any) { op["parameters"] = params }
}

func withRequestBody(description string, schema map[string]any) operationOption {
	return func(op map[string]any) {
		op["requestBody"] = map[string]any{
			"description": description,
			"required":    true,
			"content": map[string]any{
				"application/json": map[string]any{"schema": schema},
			},
		}
	}
}

type responseEntry struct {
	status string
	body   map[string]any
}

func withResponses(entries ...responseEntry) operationOption {
	return func(op map[string]any) {
		responses := make(map[string]any, len(entries))
		for _, entry := range entries {
			responses[entry.status] = entry.body
		}
		op["responses"] = responses
	}
}

func jsonResponse(status, description string, schema map[string]any) responseEntry {
	return contentResponse(status, description, "application/json", schema)
}

func contentResponse(status, description, mediaType string, schema map[string]any) responseEntry {
	return responseEntry{status: status, body: map[string]any{
		"description": description,
		"content": map[string]any{
			mediaType: map[string]any{"schema": schema},
		},
	}}
}

func emptyResponse(status, description string) responseEntry {
	return responseEntry{status: status, body: map[string]any{"description": description}}
}

func queryParam(name, schemaType, description string) map[string]any {
	return map[string]any{
		"name":        name,
		"in":          "query",
		"description": description,
		"schema":      map[string]any{"type": schemaType},
	}
}

// idPathParam declares the {id} segment every item route shares.
func idPathParam(description string) map[string]any {
	return map[string]any{
		"name":        "id",
		"in":          "path",
		"required":    true,
		"description": description,
		"schema":      map[string]any{"type": "string"},
	}
}

// --- Schemas ---

func errorSchema() map[string]any {
	return objectSchema(map[string]any{
		"error": stringProp("Human-readable failure description."),
	})
}

func skillSchema() map[string]any {
	schema := objectSchema(map[string]any{
		"id":                    stringProp("Opaque, immutable identity — the route key."),
		"slug":                  stringProp("Cosmetic kebab-case display label and SKILL.md filename."),
		"parentId":              map[string]any{"type": []any{"string", "null"}, "description": "Source skill id when this is a duplicate/fork."},
		"name":                  stringProp("Human display name."),
		"description":           stringProp("Trigger description for when an agent should use this skill."),
		"type":                  map[string]any{"type": "string", "enum": []string{"workflow", "domain-knowledge", "prompt-template"}},
		"version":               stringProp("Current published semver."),
		"visibility":            stringProp("private or team."),
		"status":                map[string]any{"type": "string", "enum": []string{"draft", "published"}},
		"tags":                  stringArrayProp("Free-form tags."),
		"content":               stringProp("Markdown body — the editable head."),
		"isAiGenerated":         map[string]any{"type": "boolean"},
		"originatingSessionIds": stringArrayProp("Source session provenance."),
		"authorId":              stringProp("Gateway-trusted subject of the creator; empty when unattributed."),
		"downloadCount":         map[string]any{"type": "integer", "description": "How many times the SKILL.md has been downloaded."},
		"createdAt":             stringProp("RFC 3339 creation time."),
		"updatedAt":             stringProp("RFC 3339 last-edit time."),
	})
	return schema
}

func skillsListSchema() map[string]any {
	return objectSchema(map[string]any{
		"items":       arrayOf(skillSchema()),
		"next_cursor": stringProp("Opaque keyset cursor; absent on the last page."),
		"counts": objectSchema(map[string]any{
			"all":    map[string]any{"type": "integer"},
			"mine":   map[string]any{"type": "integer"},
			"team":   map[string]any{"type": "integer"},
			"drafts": map[string]any{"type": "integer"},
		}),
	})
}

func skillVersionSchema() map[string]any {
	return objectSchema(map[string]any{
		"id":            stringProp("Synthetic id: <skillId>-v<versionNumber>."),
		"skillId":       stringProp("Owning skill id."),
		"versionNumber": map[string]any{"type": "integer", "description": "Monotonic publish counter starting at 1."},
		"semver":        stringProp("Published semver, e.g. 0.1.2."),
		"publishedAt":   stringProp("RFC 3339 publish time."),
		"changelog":     stringProp("Publisher-supplied change note."),
		"content":       stringProp("The immutable published snapshot."),
		"authorId":      stringProp("Subject that published the version."),
	})
}

func skillVersionsSchema() map[string]any {
	return objectSchema(map[string]any{
		"versions":   arrayOf(skillVersionSchema()),
		"totalCount": map[string]any{"type": "integer"},
	})
}

func createSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"name":                  stringProp("Display name; defaults to \"New skill\"."),
		"description":           stringProp("Trigger description."),
		"type":                  map[string]any{"type": "string", "enum": []string{"workflow", "domain-knowledge", "prompt-template"}},
		"tags":                  stringArrayProp("Free-form tags."),
		"content":               stringProp("Markdown body."),
		"originatingSessionIds": boundedStringArrayProp("Source session provenance.", maxProvenanceSessions),
		"isAiGenerated":         map[string]any{"type": "boolean", "description": "Whether an AI generation pass produced this draft."},
	})
}

func updateSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"name":                  stringProp("New display name; the slug follows it."),
		"description":           stringProp("New trigger description."),
		"type":                  map[string]any{"type": "string", "enum": []string{"workflow", "domain-knowledge", "prompt-template"}},
		"visibility":            stringProp("private or team."),
		"tags":                  stringArrayProp("Replacement tag set."),
		"content":               stringProp("New markdown body (does not publish)."),
		"originatingSessionIds": boundedStringArrayProp("Replacement provenance set; omission preserves it.", maxProvenanceSessions),
	})
}

func generateSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"sessionIds": boundedStringArrayProp("Ordered source session ids. May be empty when brief is non-empty.", maxGenerationSessions),
		"brief":      stringProp("Free-text author intent; maximum 10,000 UTF-8 bytes."),
		"feedback":   boundedStringArrayProp("Cumulative review notes, oldest first; maximum 20,000 UTF-8 bytes total.", maxFeedbackItems),
	})
}

func generatedSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"name":                  stringProp("Suggested display name."),
		"description":           stringProp("Generated trigger description."),
		"type":                  map[string]any{"type": "string", "enum": []string{"workflow", "domain-knowledge", "prompt-template"}},
		"tags":                  stringArrayProp("Generated tags."),
		"content":               stringProp("Generated markdown body."),
		"originatingSessionIds": stringArrayProp("Normalized source session ids."),
		"isAiGenerated":         map[string]any{"type": "boolean"},
	})
}

func reviseSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"content":     stringProp("Current working markdown; maximum 200,000 UTF-8 bytes."),
		"instruction": stringProp("Requested change; maximum 10,000 UTF-8 bytes."),
	})
}

func revisedSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"content": stringProp("Complete replacement markdown."),
	})
}

func publishSkillSchema() map[string]any {
	return objectSchema(map[string]any{
		"content":   stringProp("Content to snapshot; defaults to the skill's current head."),
		"changelog": stringProp("Change note recorded on the version."),
	})
}

func objectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func stringProp(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func stringArrayProp(description string) map[string]any {
	return map[string]any{
		"type":        "array",
		"description": description,
		"items":       map[string]any{"type": "string"},
	}
}

func boundedStringArrayProp(description string, maxItems int) map[string]any {
	prop := stringArrayProp(description)
	prop["maxItems"] = maxItems
	return prop
}

func arrayOf(schema map[string]any) map[string]any {
	return map[string]any{"type": "array", "items": schema}
}

// RoutePrefix is the prefix this cassette serves under, exported for tests.
func RoutePrefix(name string) string { return "/api/" + strings.TrimPrefix(name, "/") }

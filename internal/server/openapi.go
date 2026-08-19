package server

import (
	"encoding/json"
	"strings"
)

const Version = "0.4.0"

func openAPIDocument(name string) []byte {
	prefix := "/api/" + name
	paths := map[string]any{
		prefix: map[string]any{"get": operation("listSkills", "List published skills", "Returns current immutable publications only.", name,
			withParameters(queryParam("limit", "integer", "Page size (default 24, max 100)"), queryParam("cursor", "string", "Opaque keyset cursor"), queryParam("q", "string", "Search published metadata"), queryParam("scope", "string", "Attribution slice: all, mine, or team"), queryParam("sort", "string", "downloads or recently updated"), queryParam("session_id", "string", "Published skills whose current version used this source session")),
			withResponses(jsonResponse("200", "Published skills, or a session reverse lookup when session_id is present", skillsListOrSessionSchema()), jsonResponse("400", "Malformed cursor", errorSchema()), jsonResponse("500", "Listing failed", errorSchema())))},
		prefix + "/drafts": map[string]any{
			"get":  operation("listSkillDrafts", "List drafts", "Returns all mutable drafts in this cassette tenant.", name, withResponses(jsonResponse("200", "Drafts", draftsListSchema()), jsonResponse("500", "Listing failed", errorSchema()))),
			"post": operation("createSkillDraft", "Create authored draft", "Creates a stable skill identity and persisted authored draft.", name, withRequestBody("Draft fields", createDraftSchema()), withResponses(jsonResponse("201", "Created draft", draftSchema()), jsonResponse("400", "Invalid draft", errorSchema()), jsonResponse("500", "Create failed", errorSchema()))),
		},
		prefix + "/drafts/generate": map[string]any{"post": operation("generateSkillDraft", "Generate persisted draft", "Uses every nominated session or rejects the request; successful inference persists exact server-owned source provenance.", name, withRequestBody("Generation sources", generateDraftSchema()), withResponses(jsonResponse("201", "Generated draft", draftSchema()), jsonResponse("400", "Invalid input", errorSchema()), jsonResponse("404", "Source session missing", errorSchema()), jsonResponse("422", "Unusable or oversized context, or no LLM key", errorSchema()), jsonResponse("500", "Generation failed", errorSchema()), jsonResponse("501", "Core URL not configured", errorSchema())))},
		prefix + "/{id}": map[string]any{"parameters": []any{idPathParam("Skill id")},
			"get":    operation("getSkill", "Get published skill", "Returns only the current immutable publication.", name, withResponses(jsonResponse("200", "Published skill", skillSchema()), jsonResponse("404", "No publication", errorSchema()), jsonResponse("500", "Lookup failed", errorSchema()))),
			"delete": operation("deleteSkill", "Delete skill", "Deletes identity, draft, and versions; existing creator gate is unchanged.", name, withResponses(emptyResponse("204", "Deleted"), jsonResponse("403", "Forbidden", errorSchema()), jsonResponse("404", "Not found", errorSchema()), jsonResponse("500", "Delete failed", errorSchema()))),
		},
		prefix + "/{id}/draft": map[string]any{"parameters": []any{idPathParam("Skill id")},
			"get":  operation("getSkillDraft", "Get draft", "Returns the mutable working copy.", name, withResponses(jsonResponse("200", "Draft", draftSchema()), jsonResponse("404", "Draft not found", errorSchema()), jsonResponse("500", "Lookup failed", errorSchema()))),
			"post": operation("initializeSkillDraft", "Initialize draft", "Copies the current publication into a new working draft.", name, withResponses(jsonResponse("201", "Draft", draftSchema()), jsonResponse("404", "Published skill not found", errorSchema()), jsonResponse("409", "Draft already exists", errorSchema()), jsonResponse("500", "Create failed", errorSchema()))),
			"put":  operation("updateSkillDraft", "Update draft", "Conditionally changes client-editable fields; source provenance and AI attribution remain server-owned.", name, withRequestBody("Expected revision and fields", updateDraftSchema()), withResponses(jsonResponse("200", "Updated draft", draftSchema()), jsonResponse("400", "Invalid update", errorSchema()), jsonResponse("404", "Draft not found", errorSchema()), jsonResponse("409", "Stale revision", errorSchema()), jsonResponse("500", "Update failed", errorSchema()))),
		},
		prefix + "/{id}/draft/revise": map[string]any{"parameters": []any{idPathParam("Skill id")}, "post": operation("reviseSkillDraft", "Revise draft with AI", "Loads current content server-side and conditionally persists the rewrite.", name, withRequestBody("Instruction and expected revision", reviseDraftSchema()), withResponses(jsonResponse("200", "Revised draft", draftSchema()), jsonResponse("400", "Invalid instruction", errorSchema()), jsonResponse("404", "Draft not found", errorSchema()), jsonResponse("409", "Stale revision", errorSchema()), jsonResponse("422", "No LLM key", errorSchema()), jsonResponse("500", "Revision failed", errorSchema())))},
		prefix + "/{id}/publish":      map[string]any{"parameters": []any{idPathParam("Skill id")}, "post": operation("publishSkillDraft", "Publish draft", "Atomically snapshots the full draft, advances the published pointer, and consumes the draft.", name, withRequestBody("Expected revision and changelog", publishDraftSchema()), withResponses(jsonResponse("201", "Immutable version", versionSchema()), jsonResponse("400", "Invalid request", errorSchema()), jsonResponse("404", "Draft not found", errorSchema()), jsonResponse("409", "Stale revision", errorSchema()), jsonResponse("500", "Publish failed", errorSchema())))},
		prefix + "/{id}/versions":     map[string]any{"parameters": []any{idPathParam("Skill id")}, "get": operation("listSkillVersions", "List versions", "Complete immutable history, newest first.", name, withResponses(jsonResponse("200", "Versions", versionsSchema()), jsonResponse("500", "Listing failed", errorSchema())))},
		prefix + "/{id}/duplicate":    map[string]any{"parameters": []any{idPathParam("Published skill id")}, "post": operation("duplicateSkill", "Duplicate into draft", "Creates an authored child draft without copying AI or session provenance.", name, withResponses(jsonResponse("201", "Created draft", draftSchema()), jsonResponse("404", "Published skill not found", errorSchema()), jsonResponse("500", "Duplicate failed", errorSchema())))},
		prefix + "/{id}/skill.md":     map[string]any{"parameters": []any{idPathParam("Skill id")}, "get": operation("getSkillMarkdown", "Download published SKILL.md", "Renders only the current immutable publication and counts the download.", name, withResponses(contentResponse("200", "SKILL.md", "text/markdown", map[string]any{"type": "string"}), jsonResponse("404", "No publication", errorSchema()), jsonResponse("500", "Lookup failed", errorSchema())))},
	}
	document := map[string]any{"openapi": "3.1.0", "info": map[string]any{"title": "Skills cassette", "description": "Generates, drafts, publishes, and serves reusable SKILL.md skills.", "version": Version}, "x-tapes-cassette": manifest(name), "paths": paths}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return []byte(`{"error":"could not compile OpenAPI"}`)
	}
	return encoded
}

func manifest(name string) map[string]any {
	return map[string]any{
		"kind":     "cassette/v1alpha1",
		"cassette": map[string]any{"name": name, "version": Version, "display_name": "Skills", "description": "Generates, stores, versions, and serves reusable SKILL.md skills extracted from Tapes sessions.", "license": "MIT OR Apache-2.0", "homepage": "https://github.com/papercomputeco/skills-cassette", "image": "public.ecr.aws/g4e5l3z3/papercomputeco/skills-cassette:v" + Version, "port": 9998},
		"depends":  map[string]any{"core": "v1", "views": []string{}},
		"api":      map[string]any{"health": "/ping", "openapi": "/openapi", "prefix_path": "api"},
		"tables":   []map[string]any{{"name": "skills"}, {"name": "skill_drafts"}, {"name": "skill_versions"}},
		"config": []map[string]any{
			{"key": "core.url", "type": "string", "required": false, "description": "Optional Tapes core API origin for session-backed generation; brief-only generation does not require it."},
			{"key": "llm.provider", "type": "string", "default": "openai", "enum": []string{"openai", "anthropic", "ollama"}, "description": "LLM provider used to extract skills."},
			{"key": "llm.model", "type": "string", "description": "Model override; each provider has a sensible default."},
			{"key": "llm.api_key", "type": "string", "secret": true, "description": "Provider API key. Not required for ollama."},
			{"key": "llm.base_url", "type": "string", "description": "Provider base URL override for proxies and self-hosted endpoints."},
		},
	}
}

type operationOption func(map[string]any)

func operation(id, summary, description, tag string, opts ...operationOption) map[string]any {
	op := map[string]any{"operationId": id, "summary": summary, "description": description, "tags": []string{tag}}
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
		op["requestBody"] = map[string]any{"description": description, "required": true, "content": map[string]any{"application/json": map[string]any{"schema": schema}}}
	}
}

type responseEntry struct {
	status string
	body   map[string]any
}

func withResponses(entries ...responseEntry) operationOption {
	return func(op map[string]any) {
		responses := map[string]any{}
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
	return responseEntry{status, map[string]any{"description": description, "content": map[string]any{mediaType: map[string]any{"schema": schema}}}}
}
func emptyResponse(status, description string) responseEntry {
	return responseEntry{status, map[string]any{"description": description}}
}
func queryParam(name, schemaType, description string) map[string]any {
	return map[string]any{"name": name, "in": "query", "description": description, "schema": map[string]any{"type": schemaType}}
}
func idPathParam(description string) map[string]any {
	return map[string]any{"name": "id", "in": "path", "required": true, "description": description, "schema": map[string]any{"type": "string"}}
}

func errorSchema() map[string]any {
	return objectSchemaRequired(map[string]any{"error": stringProp("Failure description")}, "error")
}
func skillFields() map[string]any {
	return map[string]any{"slug": stringProp("Published slug"), "name": stringProp("Display name"), "description": stringProp("Trigger description"), "type": skillTypeProp(), "tags": stringArrayProp("Tags"), "content": stringProp("Markdown"), "isAiGenerated": map[string]any{"type": "boolean"}, "sourceSessionIds": stringArrayProp("Sessions actually supplied to generation")}
}
func skillSchema() map[string]any {
	p := skillFields()
	p["id"] = stringProp("Stable skill id")
	p["version"] = stringProp("Current semver")
	p["visibility"] = stringProp("Visibility, independent of draft state")
	p["parentId"] = map[string]any{"type": []any{"string", "null"}}
	p["authorId"] = map[string]any{"type": []any{"string", "null"}}
	p["downloadCount"] = map[string]any{"type": "integer"}
	p["createdAt"] = stringProp("RFC3339")
	p["updatedAt"] = stringProp("RFC3339")
	return objectSchemaRequired(p, "id", "slug", "name", "description", "type", "version", "visibility", "tags", "content", "isAiGenerated", "sourceSessionIds", "parentId", "authorId", "downloadCount", "createdAt", "updatedAt")
}
func draftSchema() map[string]any {
	p := skillFields()
	p["skillId"] = stringProp("Stable skill id")
	p["revision"] = map[string]any{"type": "integer", "minimum": 1}
	p["createdAt"] = stringProp("RFC3339")
	p["updatedAt"] = stringProp("RFC3339")
	return objectSchemaRequired(p, "skillId", "revision", "slug", "name", "description", "type", "tags", "content", "isAiGenerated", "sourceSessionIds", "createdAt", "updatedAt")
}
func versionSchema() map[string]any {
	p := skillFields()
	p["version"] = stringProp("Semver")
	p["versionNumber"] = map[string]any{"type": "integer"}
	p["changelog"] = stringProp("Change note")
	p["authorId"] = map[string]any{"type": []any{"string", "null"}}
	p["publishedAt"] = stringProp("RFC3339")
	return objectSchemaRequired(p, "version", "versionNumber", "slug", "name", "description", "type", "tags", "content", "isAiGenerated", "sourceSessionIds", "changelog", "authorId", "publishedAt")
}
func skillsListSchema() map[string]any {
	counts := objectSchemaRequired(map[string]any{"all": map[string]any{"type": "integer"}, "mine": map[string]any{"type": "integer"}, "team": map[string]any{"type": "integer"}}, "all", "mine", "team")
	return objectSchemaRequired(map[string]any{"items": arrayOf(skillSchema()), "next_cursor": stringProp("Next cursor"), "counts": counts}, "items", "counts")
}
func sessionSkillsSchema() map[string]any {
	return objectSchemaRequired(map[string]any{"items": arrayOf(skillSchema())}, "items")
}
func skillsListOrSessionSchema() map[string]any {
	return map[string]any{"anyOf": []any{skillsListSchema(), sessionSkillsSchema()}}
}
func draftsListSchema() map[string]any {
	return objectSchemaRequired(map[string]any{"items": arrayOf(draftSchema()), "totalCount": map[string]any{"type": "integer"}}, "items", "totalCount")
}
func versionsSchema() map[string]any {
	return objectSchemaRequired(map[string]any{"versions": arrayOf(versionSchema()), "totalCount": map[string]any{"type": "integer"}}, "versions", "totalCount")
}
func createDraftSchema() map[string]any {
	return objectSchema(map[string]any{"name": stringProp("Display name"), "description": stringProp("Description"), "type": skillTypeProp(), "tags": stringArrayProp("Tags"), "content": stringProp("Markdown")})
}
func generateDraftSchema() map[string]any {
	sessions := boundedStringArrayProp("All source sessions", maxGenerationSessions)
	sessions["minItems"] = 1
	brief := stringProp("Author brief; max 10000 UTF-8 bytes")
	brief["minLength"] = 1
	schema := objectSchema(map[string]any{"sessionIds": sessions, "brief": brief})
	schema["anyOf"] = []any{map[string]any{"required": []string{"sessionIds"}}, map[string]any{"required": []string{"brief"}}}
	return schema
}
func updateDraftSchema() map[string]any {
	return objectSchemaRequired(map[string]any{"revision": map[string]any{"type": "integer", "minimum": 1}, "name": stringProp("Name"), "description": stringProp("Description"), "type": skillTypeProp(), "tags": stringArrayProp("Tags"), "content": stringProp("Markdown")}, "revision")
}
func reviseDraftSchema() map[string]any {
	instruction := stringProp("Revision instruction")
	instruction["minLength"] = 1
	return objectSchemaRequired(map[string]any{"revision": map[string]any{"type": "integer", "minimum": 1}, "instruction": instruction}, "revision", "instruction")
}
func publishDraftSchema() map[string]any {
	return objectSchemaRequired(map[string]any{"revision": map[string]any{"type": "integer", "minimum": 1}, "changelog": stringProp("Change note")}, "revision")
}
func objectSchema(properties map[string]any) map[string]any {
	return objectSchemaRequired(properties)
}
func objectSchemaRequired(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}
func skillTypeProp() map[string]any {
	return map[string]any{"type": "string", "enum": []string{"workflow", "domain-knowledge", "prompt-template"}}
}
func stringProp(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}
func stringArrayProp(description string) map[string]any {
	return map[string]any{"type": "array", "description": description, "items": map[string]any{"type": "string"}}
}
func boundedStringArrayProp(description string, maxItems int) map[string]any {
	p := stringArrayProp(description)
	p["maxItems"] = maxItems
	return p
}
func arrayOf(schema map[string]any) map[string]any {
	return map[string]any{"type": "array", "items": schema}
}
func RoutePrefix(name string) string { return "/api/" + strings.TrimPrefix(name, "/") }

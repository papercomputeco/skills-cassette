package server

import (
	"encoding/json"
	"strings"
)

var Version = Placeholder

const Placeholder = "0.0.0"

const cassetteDescription = "Stores immutable skill revisions and orchestrates durable generation from Tapes sessions."

// openAPIDocument renders the admitted local cassette surface. Every operation
// remains beneath /api/<name>; core republishes the same paths beneath the
// tenant-local cassette gateway.
func openAPIDocument(name string) []byte {
	prefix := "/api/" + name
	paths := map[string]any{}
	revisionPaths := revisionOpenAPIPaths(prefix, name)
	// Creator identity is an authorization input, not public revision or
	// generation-history data. Keep it out of every reused revision schema.
	removeOpenAPIProperty(revisionPaths, "creatorSubject")
	for path, item := range revisionPaths {
		paths[path] = item
	}
	for path, item := range generationOpenAPIPaths(prefix, name) {
		paths[path] = item
	}
	document := map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title": "Skills cassette", "description": cassetteDescription, "version": Version,
		},
		"x-tapes-cassette": manifest(name),
		"paths":            paths,
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return []byte(`{"error":"could not compile this cassette's OpenAPI document: ` +
			strings.ReplaceAll(err.Error(), `"`, `'`) + `"}`)
	}
	return encoded
}

// manifest is the served JSON twin of cassette.toml.
func manifest(name string) map[string]any {
	return map[string]any{
		"kind": "cassette/v1alpha1",
		"cassette": map[string]any{
			"name": name, "version": Version, "display_name": "Skills",
			"description": cassetteDescription,
			"license":     "MIT OR Apache-2.0", "homepage": "https://github.com/papercomputeco/skills-cassette",
			"image": "public.ecr.aws/g4e5l3z3/papercomputeco/skills-cassette:v" + Version,
			"port":  9998,
		},
		"depends": map[string]any{"core": "v1", "views": []string{}},
		"publishes": map[string]any{"views": []string{
			"skills_contract_v1.skill_identities",
			"skills_contract_v1.revision_identities",
		}},
		"api": map[string]any{
			"health": "/ping", "openapi": "/openapi", "prefix_path": "api",
		},
		"tables": []map[string]any{
			{"name": "skills"},
			{"name": "skill_revisions"},
			{"name": "skill_revision_visibility"},
			{"name": "skill_generations"},
			{"name": "generation_sessions"},
			{"name": "generation_candidates"},
			{"name": "candidate_evaluations"},
			{"name": "generation_diagnostics"},
		},
		"config": []map[string]any{
			{
				"key": "core.url", "type": "string", "required": true,
				"description": "Tapes core API origin used for trace reads and tenant-local skills-evaluator calls.",
			},
			{
				"key": "llm.provider", "type": "string", "default": "openai",
				"enum":        []string{"openai", "anthropic", "ollama"},
				"description": "LLM provider used to generate independent per-session candidates.",
			},
			{
				"key": "llm.model", "type": "string",
				"description": "Model override; each provider has a sensible default.",
			},
			{
				"key": "llm.api_key", "type": "string", "secret": true,
				"description": "Provider API key. Not required for ollama.",
			},
			{
				"key": "llm.base_url", "type": "string",
				"description": "Provider base URL override for proxies and self-hosted endpoints.",
			},
			{
				"key": "generation.worker_concurrency", "type": "int", "default": 2, "min": 1, "max": 64,
				"description": "Maximum concurrently claimed generations in this cassette process.",
			},
			{
				"key": "generation.max_sessions", "type": "int", "default": 8, "min": 1, "max": 100,
				"description": "Maximum selected sessions processed by one asynchronous generation.",
			},
			{
				"key": "generation.candidate_concurrency", "type": "int", "default": 2, "min": 1, "max": 100,
				"description": "Requested candidate-generation and candidate-evaluation external-call concurrency per generation; the effective value is deterministically min(candidate_concurrency, max_sessions).",
			},
			{
				"key": "generation.poll_interval_ms", "type": "int", "default": 250, "min": 10, "max": 60000,
				"description": "Initial empty-queue and polling failure delay in milliseconds.",
			},
			{
				"key": "generation.max_poll_interval_ms", "type": "int", "default": 5000, "min": 10, "max": 300000,
				"description": "Maximum exponential empty-queue and polling failure delay in milliseconds.",
			},
			{
				"key": "generation.lease_duration_ms", "type": "int", "default": 120000, "min": 1000, "max": 3600000,
				"description": "Generation claim lease duration in milliseconds.",
			},
			{
				"key": "generation.heartbeat_interval_ms", "type": "int", "default": 30000, "min": 100, "max": 600000,
				"description": "Claim renewal interval in milliseconds; it must remain shorter than the lease duration.",
			},
			{
				"key": "generation.processing_timeout_ms", "type": "int", "default": 300000, "min": 1000, "max": 3600000,
				"description": "Maximum wall time for one claimed generation attempt in milliseconds.",
			},
			{
				"key": "generation.drain_timeout_ms", "type": "int", "default": 10000, "min": 1000, "max": 300000,
				"description": "Maximum graceful worker drain wait in milliseconds after shutdown.",
			},
			{
				"key": "generation.retry_backoff_ms", "type": "int", "default": 1000, "min": 10, "max": 300000,
				"description": "Initial durable retry delay before bounded jitter in milliseconds.",
			},
			{
				"key": "generation.max_retry_backoff_ms", "type": "int", "default": 30000, "min": 10, "max": 3600000,
				"description": "Maximum durable retry delay before bounded jitter in milliseconds.",
			},
			{
				"key": "generation.max_attempts", "type": "int", "default": 3, "min": 1, "max": 10,
				"description": "Maximum durable claim attempts before terminal failure.",
			},
			{
				"key": "generation.max_transcript_bytes", "type": "int", "default": 1048576, "min": 4096, "max": 1048576,
				"description": "Maximum rendered bytes supplied to any one candidate inference.",
			},
			{
				"key": "filters", "type": "json",
				"description": "External attachment-view filters: a JSON list of {param, view, type_value, normalize} entries, each wiring one repeatable skills-list query param to a deployment-granted view of the canonical attachment shape (primitive_type, primitive_id, value). Normalize verbs: trim, nfc, casefold. Absent: the capability is off.",
			},
		},
		"entities": []map[string]any{{
			"type": "skill", "id_kind": "uuid", "display_name": "Skill",
		}},
	}
}

type operationOption func(map[string]any)

func operation(id, summary, description, tag string, opts ...operationOption) map[string]any {
	op := map[string]any{
		"operationId": id, "summary": summary, "description": description, "tags": []string{tag},
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
			"description": description, "required": true,
			"content": map[string]any{"application/json": map[string]any{"schema": schema}},
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
		"content":     map[string]any{mediaType: map[string]any{"schema": schema}},
	}}
}

func boundedStringQueryParam(name, description string, maxCodePoints int) map[string]any {
	return map[string]any{
		"name": name, "in": "query", "description": description,
		"schema": map[string]any{"type": "string", "maxLength": maxCodePoints},
	}
}

func enumStringQueryParam(name, description string, values ...string) map[string]any {
	return map[string]any{
		"name": name, "in": "query", "description": description,
		"schema": map[string]any{"type": "string", "enum": values},
	}
}

func pathParam(name, description string) map[string]any {
	return map[string]any{
		"name": name, "in": "path", "required": true, "description": description,
		"schema": map[string]any{
			"type": "string", "format": "uuid", "maxLength": maxUUIDRepresentationCodePoints,
		},
	}
}

func lifecycleErrorSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"error": closedObjectSchema(map[string]any{
			"code":       boundedStringProp("Stable machine-readable error code.", maxLifecycleCodeCodePoints),
			"message":    boundedStringProp("Curated human-readable message.", maxLifecycleMessageCodePoints),
			"resourceId": uuidProp("Safe resource UUID when the caller may know it."),
		}, "code", "message"),
	}, "error")
}

func objectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func closedObjectSchema(properties map[string]any, required ...string) map[string]any {
	schema := objectSchema(properties)
	schema["additionalProperties"] = false
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func dateTimeProp(description string) map[string]any {
	return map[string]any{
		"type": "string", "format": "date-time", "maxLength": 64,
		"description": description,
	}
}

func nullableDateTimeProp(description string) map[string]any {
	return map[string]any{
		"type": []any{"string", "null"}, "format": "date-time", "maxLength": 64,
		"description": description,
	}
}

func boundedStringProp(description string, maxCodePoints int) map[string]any {
	return map[string]any{"type": "string", "maxLength": maxCodePoints, "description": description}
}

func nonEmptyBoundedStringProp(description string, maxCodePoints int) map[string]any {
	schema := boundedStringProp(description, maxCodePoints)
	schema["minLength"] = 1
	return schema
}

func uuidProp(description string) map[string]any {
	return map[string]any{
		"type": "string", "format": "uuid", "maxLength": maxUUIDRepresentationCodePoints,
		"description": description,
	}
}

func nullableUUIDProp(description string) map[string]any {
	return map[string]any{
		"type": []any{"string", "null"}, "format": "uuid", "maxLength": maxUUIDRepresentationCodePoints,
		"description": description,
	}
}

func removeOpenAPIProperty(value any, property string) {
	switch typed := value.(type) {
	case map[string]any:
		if properties, ok := typed["properties"].(map[string]any); ok {
			delete(properties, property)
		}
		if required, ok := typed["required"].([]string); ok {
			filtered := required[:0]
			for _, name := range required {
				if name != property {
					filtered = append(filtered, name)
				}
			}
			typed["required"] = filtered
		}
		for _, child := range typed {
			removeOpenAPIProperty(child, property)
		}
	case []any:
		for _, child := range typed {
			removeOpenAPIProperty(child, property)
		}
	}
}

func boundedStringArrayProp(description string, maxItems int) map[string]any {
	return map[string]any{
		"type": "array", "description": description, "maxItems": maxItems,
		"uniqueItems": true,
		"items": map[string]any{
			"type": "string", "minLength": 1, "maxLength": maxLifecycleIdentityCodePoints,
		},
	}
}

func boundedArrayOf(schema map[string]any, maxItems int) map[string]any {
	return map[string]any{"type": "array", "items": schema, "maxItems": maxItems}
}

// RoutePrefix is the prefix this cassette serves under, exported for tests.
func RoutePrefix(name string) string { return "/api/" + strings.TrimPrefix(name, "/") }

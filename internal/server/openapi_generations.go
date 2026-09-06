package server

import "github.com/papercomputeco/skills-cassette/internal/storage"

const (
	maxGenerationEvaluationCriteria         = 100
	maxGenerationCandidateInsights          = storage.MaxGenerationCandidateInsights
	maxGenerationCandidateInsightCodePoints = storage.MaxGenerationCandidateInsightRunes
	maxGenerationEvaluatorFindings          = 50
	maxGenerationEvaluatorStrengths         = 50
	maxGenerationEvaluatorTextCodePoints    = 4000
	maxGenerationEvaluatorPathCodePoints    = 64 << 10
)

func generationOpenAPIPaths(prefix, tag string) map[string]any {
	skillID := pathParam("skillId", "Stable skill UUID.")
	generationID := pathParam("generationId", "Public asynchronous generation UUID.")
	return map[string]any{
		prefix + "/{skillId}/generations": map[string]any{
			"parameters": []any{skillID},
			"get": operation("listSkillGenerations", "List skill generations",
				"Returns one storage-authorized bounded newest-first page. Nonterminal, failed, canceled, and private-result generations are creator-only; completed history follows the result revision's current public-or-creator visibility.", tag,
				withParameters(
					boundedIntegerQueryParam("limit", "Generation page size.", 1, 100, 20),
					boundedStringQueryParam("cursor", "Opaque creation-time/UUID keyset cursor.", maxSkillCursorCodePoints),
				),
				withResponses(
					jsonResponse("200", "Retained generation history page", generationListSchema()),
					jsonResponse("400", "Malformed limit or cursor", lifecycleErrorSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Skill not found", lifecycleErrorSchema()),
					jsonResponse("500", "Listing failed", lifecycleErrorSchema()),
				)),
			"post": operation("createSkillGeneration", "Start skill generation",
				"Commits stable skill/base UUIDs, a complete generation seed, author context, ordered selected sessions, creator, and server-owned rubric before returning 202. Concurrent nonterminal generations are allowed.", tag,
				withRequestBody("Immutable generation input", createGenerationSchema()),
				withResponses(
					jsonResponse("202", "Durably queued generation", generationSchema()),
					jsonResponse("400", "Invalid request", lifecycleErrorSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Skill or accessible same-skill base revision not found", lifecycleErrorSchema()),
					jsonResponse("500", "Creation failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/generations/{generationId}": map[string]any{
			"parameters": []any{skillID, generationID},
			"get": operation("getSkillGeneration", "Get skill generation progress",
				"Returns one exact storage-authorized generation with bounded candidates, judgments, ranking inputs, and curated diagnostics. Completed history follows current result-revision visibility; all other history is creator-only.", tag,
				withResponses(
					jsonResponse("200", "Bounded generation progress", generationSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Generation not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("500", "Lookup failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/generations/{generationId}": map[string]any{
			"parameters": []any{generationID},
			"get": operation("getSkillGenerationById", "Get generation progress by ID",
				"Applies the same storage-level result-revision visibility rule as nested reads without requiring clients to scan skill histories.", tag,
				withResponses(
					jsonResponse("200", "Bounded generation progress", generationSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Generation not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("500", "Lookup failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/generations/{generationId}/cancellations": map[string]any{
			"parameters": []any{skillID, generationID},
			"post": operation("cancelSkillGeneration", "Cancel generation",
				"Creator-only operation that cooperatively cancels nonterminal work, clears its claim, fences later writes, and retains already-persisted artifacts.", tag,
				withResponses(
					jsonResponse("200", "Canceled generation", generationSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Generation not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("409", "Generation is already terminal", lifecycleErrorSchema()),
					jsonResponse("500", "Cancellation failed", lifecycleErrorSchema()),
				)),
		},
	}
}

func generationListSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"generations": boundedArrayOf(generationSummarySchema(), 100),
		"nextCursor":  boundedStringProp("Opaque keyset cursor; absent on the final page.", maxSkillCursorCodePoints),
	}, "generations")
}

func generationSummarySchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":               uuidProp("Immutable generation UUID."),
		"skillId":          uuidProp("Stable target skill UUID."),
		"baseRevisionId":   nullableUUIDProp("Exact base revision captured at creation."),
		"status":           generationStatusSchema(),
		"resultRevisionId": nullableUUIDProp("Creator-private result revision appended on completion."),
		"failure":          generationFailureSchema(),
		"attemptCount":     map[string]any{"type": "integer", "minimum": 0},
		"createdAt":        dateTimeProp("RFC 3339 creation time."),
		"updatedAt":        dateTimeProp("RFC 3339 latest persisted progress time."),
		"startedAt":        nullableDateTimeProp("RFC 3339 first claim time."),
		"completedAt":      nullableDateTimeProp("RFC 3339 terminal transition time."),
	}, "id", "skillId", "baseRevisionId", "status", "resultRevisionId", "failure", "attemptCount",
		"createdAt", "updatedAt", "startedAt", "completedAt")
}

func generationSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":                      uuidProp("Immutable generation UUID."),
		"skillId":                 uuidProp("Stable target skill UUID."),
		"baseRevisionId":          nullableUUIDProp("Exact accessible same-skill base revision captured at creation."),
		"status":                  generationStatusSchema(),
		"input":                   revisionSnapshotSchema(),
		"authorContext":           boundedStringProp("Durable author guidance captured at creation.", maxGenerationAuthorContextCodePoints),
		"selectedSessionIds":      boundedStringArrayProp("Intended source sessions in caller order.", maxGenerationSelectedSessions),
		"sourceSessionIds":        boundedStringArrayProp("Sources that produced retained candidates.", maxGenerationSelectedSessions),
		"evaluatorProfile":        boundedStringProp("Immutable ranking criteria profile.", maxLifecycleIdentityCodePoints),
		"evaluatorProfileVersion": boundedStringProp("Immutable criteria profile version.", maxLifecycleIdentityCodePoints),
		"evaluationCriteria":      boundedArrayOf(generationEvaluationCriterionSchema(), maxGenerationEvaluationCriteria),
		"winnerCandidateId":       nullableUUIDProp("Deterministically selected candidate."),
		"resultCandidateId":       nullableUUIDProp("Candidate used for the result revision."),
		"resultRevisionId":        nullableUUIDProp("Creator-private result revision appended on completion."),
		"failure":                 generationFailureSchema(),
		"sessions":                boundedArrayOf(generationSessionSchema(), maxGenerationSelectedSessions),
		"candidates":              boundedArrayOf(generationCandidateSchema(), maxGenerationCandidates),
		"evaluations":             boundedArrayOf(candidateEvaluationSchema(), maxGenerationEvaluations),
		"diagnostics":             boundedArrayOf(generationDiagnosticSchema(), maxGenerationDiagnostics),
		"attemptCount":            map[string]any{"type": "integer", "minimum": 0},
		"createdAt":               dateTimeProp("RFC 3339 creation time."),
		"updatedAt":               dateTimeProp("RFC 3339 latest persisted progress time."),
		"startedAt":               nullableDateTimeProp("RFC 3339 first claim time."),
		"completedAt":             nullableDateTimeProp("RFC 3339 terminal transition time."),
	}, "id", "skillId", "baseRevisionId", "status", "input", "authorContext", "selectedSessionIds",
		"sourceSessionIds", "evaluatorProfile", "evaluatorProfileVersion", "evaluationCriteria",
		"winnerCandidateId", "resultCandidateId", "resultRevisionId", "failure", "sessions",
		"candidates", "evaluations", "diagnostics", "attemptCount", "createdAt", "updatedAt",
		"startedAt", "completedAt")
}

func createGenerationSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"baseRevisionId": nullableUUIDProp("Optional accessible same-skill base revision UUID."),
		"input":          revisionSnapshotSchema(),
		"authorContext":  boundedStringProp("Bounded author guidance.", maxGenerationAuthorContextCodePoints),
		"selectedSessionIds": map[string]any{
			"type": "array", "maxItems": maxGenerationSelectedSessions, "uniqueItems": true,
			"items": map[string]any{"type": "string", "minLength": 1, "maxLength": maxLifecycleIdentityCodePoints},
		},
	}, "input", "authorContext", "selectedSessionIds")
}

func generationFailureSchema() map[string]any {
	schema := closedObjectSchema(map[string]any{
		"code":    boundedStringProp("Stable failure code.", maxLifecycleCodeCodePoints),
		"message": boundedStringProp("Curated failure message.", maxLifecycleMessageCodePoints),
	}, "code", "message")
	schema["type"] = []any{"object", "null"}
	return schema
}

func generationStatusSchema() map[string]any {
	return map[string]any{"type": "string", "enum": []string{
		"queued", "generating_candidates", "evaluating_candidates", "synthesizing",
		"completed", "canceled", "failed",
	}}
}

func generationEvaluationCriterionSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":          boundedStringProp("Stable criterion ID.", maxLifecycleIdentityCodePoints),
		"kind":        map[string]any{"type": "string", "enum": []string{"structure", "content", "output-property"}},
		"description": boundedStringProp("Caller-owned expectation.", maxLifecycleMessageCodePoints),
		"weight":      map[string]any{"type": "integer", "minimum": 1, "maximum": 3},
	}, "id", "kind", "description", "weight")
}

func generationSessionSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"sessionId": boundedStringProp("Selected source session ID.", maxLifecycleIdentityCodePoints),
		"ordinal":   map[string]any{"type": "integer", "minimum": 0, "maximum": maxGenerationSelectedSessions - 1},
		"status": map[string]any{"type": "string", "enum": []string{
			"pending", "transcript_failed", "candidate_failed", "candidate_ready", "evaluation_failed", "evaluated",
		}},
		"candidateId":    nullableUUIDProp("Candidate produced by this source."),
		"diagnosticCode": nullableBoundedStringProp("Stable source outcome code.", maxLifecycleCodeCodePoints),
	}, "sessionId", "ordinal", "status", "candidateId", "diagnosticCode")
}

func generationCandidateSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":               uuidProp("Immutable candidate UUID."),
		"ordinal":          map[string]any{"type": "integer", "minimum": 0, "maximum": maxGenerationCandidates - 1},
		"kind":             map[string]any{"type": "string", "enum": []string{"session", "context", "synthesis"}},
		"sourceSessionIds": boundedStringArrayProp("Effective source references.", maxGenerationSelectedSessions),
		"snapshot":         revisionSnapshotSchema(),
		"insights":         generationCandidateInsightsSchema(),
		"bundleSha256":     map[string]any{"type": "string", "minLength": 64, "maxLength": 64, "pattern": "^[a-f0-9]{64}$"},
	}, "id", "ordinal", "kind", "sourceSessionIds", "snapshot", "insights", "bundleSha256")
}

func candidateEvaluationSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":             uuidProp("Persisted evaluation UUID."),
		"candidateId":    uuidProp("Evaluated candidate UUID."),
		"profile":        boundedStringProp("Evaluator profile.", maxLifecycleIdentityCodePoints),
		"profileVersion": boundedStringProp("Evaluator profile version.", maxLifecycleIdentityCodePoints),
		"evaluatorVersion": boundedStringProp(
			"Evaluator service version.", storage.MaxCandidateEvaluatorVersionCodePoints,
		),
		"score":                map[string]any{"type": []any{"number", "null"}, "minimum": 0, "maximum": 1},
		"decision":             map[string]any{"type": "string", "enum": []string{"pass", "revise"}},
		"criticalFindingCount": map[string]any{"type": "integer", "minimum": 0},
		"warningFindingCount":  map[string]any{"type": "integer", "minimum": 0},
		"criterionResults":     boundedArrayOf(generationCriterionResultSchema(), maxGenerationEvaluationCriteria),
		"findings":             boundedArrayOf(generationFindingSchema(), maxGenerationEvaluatorFindings),
		"strengths": map[string]any{
			"type": "array", "maxItems": maxGenerationEvaluatorStrengths,
			"items": boundedStringProp("Bounded evaluator strength.", maxGenerationEvaluatorTextCodePoints),
		},
	}, "id", "candidateId", "profile", "profileVersion", "evaluatorVersion", "score", "decision",
		"criticalFindingCount", "warningFindingCount", "criterionResults", "findings", "strengths")
}

func generationCandidateInsightsSchema() map[string]any {
	schema := boundedArrayOf(generationCandidateInsightSchema(), maxGenerationCandidateInsights)
	schema["x-max-json-bytes"] = storage.MaxGenerationCandidateInsightsJSONBytes
	return schema
}

func generationCandidateInsightSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"kind":     boundedStringProp("Insight classification.", maxGenerationCandidateInsightCodePoints),
		"summary":  boundedStringProp("Reusable insight summary.", maxGenerationCandidateInsightCodePoints),
		"evidence": boundedStringProp("Bounded evidence explanation without a transcript.", maxGenerationCandidateInsightCodePoints),
	}, "kind", "summary", "evidence")
}

func generationCriterionResultSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"criterion_id": boundedStringProp("Caller-owned criterion ID.", maxLifecycleIdentityCodePoints),
		"weight":       map[string]any{"type": "integer", "minimum": 1, "maximum": 3},
		"passed":       map[string]any{"type": "boolean"},
		"rationale":    boundedStringProp("Bounded criterion rationale.", maxGenerationEvaluatorTextCodePoints),
	}, "criterion_id", "weight", "passed", "rationale")
}

func generationFindingSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"rule_id": boundedStringProp("Evaluator rule ID.", maxLifecycleIdentityCodePoints),
		"severity": map[string]any{
			"type": "string", "enum": []string{"info", "warn", "warning", "critical"},
		},
		"message": boundedStringProp("Bounded finding message.", maxGenerationEvaluatorTextCodePoints),
		"file":    boundedStringProp("Optional related bundle path.", maxGenerationEvaluatorPathCodePoints),
		"line":    map[string]any{"type": "integer", "minimum": 0},
	}, "severity", "message")
}

func generationDiagnosticSchema() map[string]any {
	definitions := storage.GenerationDiagnosticDefinitions()
	stages := make([]string, 0, len(definitions))
	codes := make([]string, 0, len(definitions))
	messages := make([]string, 0, len(definitions))
	seenStages := map[string]struct{}{}
	seenCodes := map[string]struct{}{}
	seenMessages := map[string]struct{}{}
	alternatives := make([]any, 0, len(definitions))
	for _, definition := range definitions {
		if _, exists := seenStages[definition.Stage]; !exists {
			seenStages[definition.Stage] = struct{}{}
			stages = append(stages, definition.Stage)
		}
		if _, exists := seenCodes[definition.Code]; !exists {
			seenCodes[definition.Code] = struct{}{}
			codes = append(codes, definition.Code)
		}
		if _, exists := seenMessages[definition.Message]; !exists {
			seenMessages[definition.Message] = struct{}{}
			messages = append(messages, definition.Message)
		}
		alternatives = append(alternatives, map[string]any{
			"properties": map[string]any{
				"stage":     map[string]any{"const": definition.Stage},
				"code":      map[string]any{"const": definition.Code},
				"message":   map[string]any{"const": definition.Message},
				"retryable": map[string]any{"const": definition.Retryable},
			},
			"required": []string{"stage", "code", "message", "retryable"},
		})
	}
	schema := closedObjectSchema(map[string]any{
		"sessionId":   nullableBoundedStringProp("Related selected source.", maxLifecycleIdentityCodePoints),
		"candidateId": nullableUUIDProp("Related candidate."),
		"stage": map[string]any{
			"type": "string", "maxLength": maxLifecycleStageCodePoints, "enum": stages,
		},
		"code": map[string]any{
			"type": "string", "maxLength": maxLifecycleCodeCodePoints, "enum": codes,
		},
		"message": map[string]any{
			"type": "string", "maxLength": maxLifecycleMessageCodePoints, "enum": messages,
		},
		"retryable": map[string]any{"type": "boolean"},
		"createdAt": dateTimeProp("RFC 3339 diagnostic time."),
	}, "sessionId", "candidateId", "stage", "code", "message", "retryable", "createdAt")
	schema["oneOf"] = alternatives
	return schema
}

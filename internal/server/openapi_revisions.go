package server

func revisionOpenAPIPaths(prefix, tag string) map[string]any {
	skillID := pathParam("skillId", "Stable skill UUID.")
	revisionID := pathParam("revisionId", "Globally unique immutable revision UUID.")
	return map[string]any{
		prefix: map[string]any{
			"get": operation("listEffectiveSkills", "List effective skills",
				"Returns at most one viewer-aware card per stable skill. Explicit latest wins; otherwise the newest public revision is effective. A creator-private card is used only when no public revision exists. session_id retains the provenance reverse lookup; deployment-armed attachment filters remain storage-scoped and apply to both rows and counts.", tag,
				withParameters(
					boundedIntegerQueryParam("limit", "Page size.", 1, maxSkillsLimit, defaultSkillsLimit),
					boundedStringQueryParam("cursor", "Opaque effective-skill keyset cursor. A cursor that recorded the sort it was cut under is rejected under another sort; cursors from replicas that recorded none are accepted under either.", maxSkillCursorCodePoints),
					boundedStringQueryParam("q", "Search over revision name, description, and tags.", maxSkillQueryCodePoints),
					enumStringQueryParam("scope", "Which slice to return.", "all", "mine", "team"),
					enumStringQueryParam("sort", "Sort by newest card revision or download count.", "recent", "downloads"),
					boundedStringQueryParam("session_id", "Return effective skills with accessible provenance from this session.", maxLifecycleIdentityCodePoints),
				),
				withResponses(
					jsonResponse("200", "One bounded page of effective skill cards", effectiveSkillsListSchema()),
					jsonResponse("400", "Malformed query or cursor", lifecycleErrorSchema()),
					jsonResponse("401", "Authenticated scope requires a caller identity", lifecycleErrorSchema()),
					jsonResponse("503", "An armed external attachment view is unreadable", lifecycleErrorSchema()),
					jsonResponse("500", "Listing failed", lifecycleErrorSchema()),
				)),
			"post": operation("resolveSkillIdentity", "Create or resolve a skill identity",
				"Normalizes the requested slug and returns the one stable tenant-local UUID. A conflict returns identity metadata only and never discloses another creator's private revisions.", tag,
				withRequestBody("Normalized identity request", resolveSkillSchema()),
				withResponses(
					jsonResponse("200", "Resolved stable skill identity", skillIdentitySchema()),
					jsonResponse("400", "Invalid slug", lifecycleErrorSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("500", "Identity resolution failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}": map[string]any{
			"parameters": []any{skillID},
			"get": operation("getEffectiveSkill", "Get an effective skill",
				"Returns stable identity, effective public revision, the viewer's newest private continuation when present, and the selected card revision. Other creators' private metadata is absent.", tag,
				withResponses(
					jsonResponse("200", "Viewer-aware effective skill", effectiveSkillSchema()),
					jsonResponse("404", "Skill not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("500", "Lookup failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/skill.md": map[string]any{
			"parameters": []any{skillID},
			"get": operation("getSkillMarkdown", "Download effective SKILL.md",
				"Renders the effective public revision in the preserved on-disk SKILL.md format. Download counting is best-effort; private content is never selected as the unversioned distribution fallback.", tag,
				withResponses(
					contentResponse("200", "Rendered SKILL.md", "text/markdown", map[string]any{
						"type": "string", "maxLength": maxSkillMarkdownOutputCodePoints,
						"x-maxBytes": maxSkillMarkdownOutputBytes,
					}),
					jsonResponse("404", "Skill not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("409", "Only a creator-private revision exists", lifecycleErrorSchema()),
					jsonResponse("500", "Lookup failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/revisions": map[string]any{
			"parameters": []any{skillID},
			"get": operation("listSkillRevisions", "List accessible revisions",
				"Returns public revisions plus only the caller's private revisions in deterministic descending sequence/UUID order. Cursor identity is the final sequenceNumber and revision UUID from the prior page.", tag,
				withParameters(
					boundedIntegerQueryParam("limit", "Revision page size.", 1, maxRevisionListLimit, storageDefaultRevisionListLimit),
					boundedStringQueryParam("cursor", "Opaque revision sequence/UUID keyset cursor.", maxSkillCursorCodePoints),
				),
				withResponses(
					jsonResponse("200", "One bounded revision page", revisionsListSchema()),
					jsonResponse("400", "Malformed page request", lifecycleErrorSchema()),
					jsonResponse("404", "Skill not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("500", "Listing failed", lifecycleErrorSchema()),
				)),
			"post": operation("appendSkillRevision", "Save a new private revision",
				"Appends one complete immutable snapshot with a caller-stable idempotency key. The server allocates UUID, sequence, decimal version, creator, hash, origin, and private visibility atomically. Visibility and latest are separate operations.", tag,
				withRequestBody("Complete immutable revision append", appendRevisionSchema()),
				withResponses(
					jsonResponse("201", "Created or idempotently reconciled private revision", revisionSchema()),
					jsonResponse("400", "Invalid or oversized revision", lifecycleErrorSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Skill or lineage revision not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("409", "Revision identity or sequence conflict", lifecycleErrorSchema()),
					jsonResponse("500", "Append failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/revisions/{revisionId}": map[string]any{
			"parameters": []any{skillID, revisionID},
			"get": operation("getSkillRevision", "Get an exact revision",
				"Returns an exact immutable UUID revision only when it is public or creator-owned. Every inaccessible case behaves as not found.", tag,
				withResponses(
					jsonResponse("200", "Exact accessible revision", revisionSchema()),
					jsonResponse("404", "Revision not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("500", "Lookup failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/revisions/{revisionId}/visibility": map[string]any{
			"parameters": []any{skillID, revisionID},
			"put": operation("setSkillRevisionVisibility", "Set revision visibility",
				"Idempotently changes only bounded visibility/audit metadata. Making the explicit latest private is rejected until latest is moved or cleared.", tag,
				withRequestBody("Desired visibility", setRevisionVisibilitySchema()),
				withResponses(
					jsonResponse("200", "Updated safe visibility metadata for the same revision", revisionVisibilityMutationSchema()),
					jsonResponse("400", "Invalid visibility request", lifecycleErrorSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Revision not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("409", "Revision is explicit latest", lifecycleErrorSchema()),
					jsonResponse("500", "Visibility update failed", lifecycleErrorSchema()),
				)),
		},
		prefix + "/{skillId}/latest": map[string]any{
			"parameters": []any{skillID},
			"put": operation("setSkillLatestRevision", "Set explicit latest",
				"Idempotently moves the explicit pointer to an already-public revision of this skill and returns the resulting effective revision.", tag,
				withRequestBody("Public revision target", setLatestSchema()),
				withResponses(
					jsonResponse("200", "Updated explicit and effective latest", latestSchema()),
					jsonResponse("400", "Invalid latest request", lifecycleErrorSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Skill or revision not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("409", "Revision is not public", lifecycleErrorSchema()),
					jsonResponse("500", "Latest update failed", lifecycleErrorSchema()),
				)),
			"delete": operation("clearSkillLatestRevision", "Clear explicit latest",
				"Idempotently clears only the explicit pointer and returns the deterministic newest-public fallback without changing revision content or visibility.", tag,
				withResponses(
					jsonResponse("200", "Cleared pointer and newly resolved effective revision", latestSchema()),
					jsonResponse("401", "Authentication required", lifecycleErrorSchema()),
					jsonResponse("404", "Skill not found or inaccessible", lifecycleErrorSchema()),
					jsonResponse("500", "Latest clear failed", lifecycleErrorSchema()),
				)),
		},
	}
}

const storageDefaultRevisionListLimit = 24

func resolveSkillSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"slug": nonEmptyBoundedStringProp("Normalized tenant-local skill slug.", maxSkillSlugCodePoints),
	}, "slug")
}

func skillIdentitySchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":                       uuidProp("Stable skill UUID."),
		"slug":                     nonEmptyBoundedStringProp("Normalized tenant-local slug.", maxSkillSlugCodePoints),
		"explicitLatestRevisionId": nullableUUIDProp("Stored public latest pointer; null when automatic fallback applies."),
		"createdAt":                dateTimeProp("RFC 3339 creation time."),
	}, "id", "slug", "explicitLatestRevisionId", "createdAt")
}

func effectiveSkillSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":                       uuidProp("Stable skill UUID."),
		"slug":                     nonEmptyBoundedStringProp("Normalized tenant-local slug.", maxSkillSlugCodePoints),
		"explicitLatestRevisionId": nullableUUIDProp("Stored latest pointer only; automatic fallback does not populate it."),
		"effectiveRevision":        nullableRevisionSchema("Explicit latest or deterministic newest-public revision."),
		"newestPrivateRevision":    nullableRevisionSchema("Current viewer's newest private continuation."),
		"cardRevision":             nullableRevisionSchema("Effective public revision, otherwise the viewer's private-only card."),
		"hasNewerPrivateRevision":  map[string]any{"type": "boolean"},
		"downloadCount":            map[string]any{"type": "integer", "minimum": 0},
		"createdAt":                dateTimeProp("RFC 3339 creation time."),
		"updatedAt":                dateTimeProp("RFC 3339 card revision time."),
	}, "id", "slug", "explicitLatestRevisionId", "effectiveRevision", "newestPrivateRevision",
		"cardRevision", "hasNewerPrivateRevision", "downloadCount", "createdAt", "updatedAt")
}

func effectiveSkillsListSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"items":      boundedArrayOf(effectiveSkillSchema(), maxSkillsLimit),
		"nextCursor": boundedStringProp("Opaque keyset cursor; absent on the final page.", maxSkillCursorCodePoints),
		"counts": closedObjectSchema(map[string]any{
			"all":  map[string]any{"type": "integer", "minimum": 0},
			"mine": map[string]any{"type": "integer", "minimum": 0},
			"team": map[string]any{"type": "integer", "minimum": 0},
		}, "all", "mine", "team"),
	}, "items")
}

func revisionSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"id":                uuidProp("Globally unique immutable revision identity."),
		"skillId":           uuidProp("Stable owning skill UUID."),
		"sequenceNumber":    map[string]any{"type": "integer", "minimum": 1},
		"version":           boundedStringProp("Server-controlled v1 decimal sequence label.", 64),
		"creatorSubject":    boundedStringProp("Trusted creator attribution.", maxLifecycleIdentityCodePoints),
		"basedOnRevisionId": nullableUUIDProp("Optional same-skill base revision."),
		"sourceRevisionId":  nullableUUIDProp("Optional cross-skill duplicate/fork source."),
		"origin":            map[string]any{"type": "string", "enum": []string{"manual", "generation", "duplicate", "migrated"}},
		"snapshot":          revisionSnapshotSchema(),
		"contentSha256": map[string]any{
			"type": "string", "minLength": 64, "maxLength": 64, "pattern": "^[a-f0-9]{64}$",
		},
		"changeNote":       nullableBoundedStringProp("Bounded append note.", maxRevisionChangeNoteCodePoints),
		"generationId":     nullableUUIDProp("Generation lineage when origin is generation."),
		"visibility":       revisionVisibilitySchema(),
		"isExplicitLatest": map[string]any{"type": "boolean"},
		"createdAt":        dateTimeProp("RFC 3339 append time."),
	}, "id", "skillId", "sequenceNumber", "version", "creatorSubject", "basedOnRevisionId",
		"sourceRevisionId", "origin", "snapshot", "contentSha256", "changeNote", "generationId",
		"visibility", "isExplicitLatest", "createdAt")
}

func nullableRevisionSchema(description string) map[string]any {
	schema := revisionSchema()
	schema["type"] = []any{"object", "null"}
	schema["description"] = description
	return schema
}

func revisionSnapshotSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"name":             nonEmptyBoundedStringProp("Human display name.", maxLifecycleMessageCodePoints),
		"description":      boundedStringProp("Trigger description.", maxRevisionContentCodePoints),
		"type":             map[string]any{"type": "string", "enum": []string{"workflow", "domain-knowledge", "prompt-template"}},
		"tags":             boundedStringArrayProp("Free-form tags.", maxRevisionTags),
		"content":          boundedStringProp("Immutable SKILL.md body.", maxRevisionContentCodePoints),
		"isAiGenerated":    map[string]any{"type": "boolean"},
		"sourceSessionIds": boundedStringArrayProp("Successfully used source evidence references.", maxRevisionSources),
	}, "name", "description", "type", "tags", "content", "isAiGenerated", "sourceSessionIds")
}

func appendRevisionSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"basedOnRevisionId": nullableUUIDProp("Optional accessible same-skill base UUID."),
		"sourceRevisionId":  nullableUUIDProp("Optional accessible revision of another skill recorded as duplicate lineage; the appended revision takes origin duplicate. The snapshot is still supplied in full by the caller, typically copied from that source."),
		"snapshot":          revisionSnapshotSchema(),
		"changeNote":        nullableBoundedStringProp("Bounded append note.", maxRevisionChangeNoteCodePoints),
		"idempotencyKey":    nonEmptyBoundedStringProp("Caller-stable append retry identity.", maxLifecycleIdentityCodePoints),
	}, "snapshot", "idempotencyKey")
}

func revisionsListSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"items":      boundedArrayOf(revisionSchema(), maxRevisionListLimit),
		"nextCursor": boundedStringProp("Opaque sequence/UUID cursor; absent on the final page.", maxSkillCursorCodePoints),
	}, "items")
}

func revisionVisibilitySchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"isPublic":  map[string]any{"type": "boolean"},
		"changedAt": dateTimeProp("RFC 3339 visibility transition time."),
	}, "isPublic", "changedAt")
}

func setRevisionVisibilitySchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"isPublic": map[string]any{"type": "boolean"},
	}, "isPublic")
}

func revisionVisibilityMutationSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"revisionId": uuidProp("Revision whose visibility was changed."),
		"isPublic":   map[string]any{"type": "boolean"},
		"changedAt":  dateTimeProp("RFC 3339 visibility transition time."),
	}, "revisionId", "isPublic", "changedAt")
}

func setLatestSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"revisionId": uuidProp("Already-public same-skill revision UUID."),
	}, "revisionId")
}

func latestSchema() map[string]any {
	return closedObjectSchema(map[string]any{
		"skillId":                  uuidProp("Stable skill UUID."),
		"explicitLatestRevisionId": nullableUUIDProp("Stored explicit latest pointer."),
		"effectiveRevision":        nullableRevisionSchema("Explicit target or newest-public fallback."),
	}, "skillId", "explicitLatestRevisionId", "effectiveRevision")
}

func nullableBoundedStringProp(description string, maxCodePoints int) map[string]any {
	return map[string]any{
		"type": []any{"string", "null"}, "maxLength": maxCodePoints, "description": description,
	}
}

func boundedIntegerQueryParam(name, description string, minimum, maximum, fallback int) map[string]any {
	return map[string]any{
		"name": name, "in": "query", "description": description,
		"schema": map[string]any{
			"type": "integer", "minimum": minimum, "maximum": maximum, "default": fallback,
		},
	}
}

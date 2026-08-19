# skills-cassette

`skills-cassette` is the Tapes cassette that generates, stores, versions, and
serves reusable `SKILL.md` skills extracted from Tapes trace data.

It is an independently deployed HTTP service speaking the `cassette/v1alpha1`
contract: Tapes core fetches its OpenAPI document from `/openapi`, admits the
`x-tapes-cassette` manifest embedded there, and republishes the API served
under `/api/skills` at `/v1/cassettes/skills`. The authored twin of that
manifest lives in [`cassette.toml`](./cassette.toml) for registries and
orchestrators; the two encode one schema and produce the same canonical
manifest digest.

## Surface

The former Tapes skills routes and the cassette's draft-generation operations
live under one cassette prefix:

| Cassette-local route | Public route (through core) | Purpose |
| --- | --- | --- |
| `GET /api/skills` | `GET /v1/cassettes/skills` | Keyset-paginated immutable publications with search, attribution scopes, sort, and counts |
| `GET /api/skills?session_id=` | `GET /v1/cassettes/skills?session_id=` | Reverse lookup over current published source provenance |
| `GET/POST /api/skills/drafts` | `GET/POST /v1/cassettes/skills/drafts` | List tenant-wide drafts or create an authored persisted draft |
| `POST /api/skills/drafts/generate` | `POST /v1/cassettes/skills/drafts/generate` | Generate and persist a draft from sessions, a brief, or both |
| `GET/POST/PUT /api/skills/{id}/draft` | `GET/POST/PUT /v1/cassettes/skills/{id}/draft` | Read, initialize, or conditionally edit the working draft |
| `POST /api/skills/{id}/draft/revise` | `POST /v1/cassettes/skills/{id}/draft/revise` | Rewrite and conditionally persist current draft content |
| `POST /api/skills/{id}/publish` | `POST /v1/cassettes/skills/{id}/publish` | Snapshot the full draft, advance publication, and consume the draft atomically |
| `GET /api/skills/{id}` | `GET /v1/cassettes/skills/{id}` | Read the current immutable publication |
| `DELETE /api/skills/{id}` | `DELETE /v1/cassettes/skills/{id}` | Creator-gated delete of identity, draft, and history |
| `GET /api/skills/{id}/skill.md` | `GET /v1/cassettes/skills/{id}/skill.md` | Download current published SKILL.md (counts a download) |
| `GET /api/skills/{id}/versions` | `GET /v1/cassettes/skills/{id}/versions` | Complete immutable published history |
| `POST /api/skills/{id}/duplicate` | `POST /v1/cassettes/skills/{id}/duplicate` | Fork a publication into a fresh authored draft |

`/ping` and `/openapi` are the process anchors core probes and fetches; they
are not part of the proxied API.

The cassette owns `skills` identities, mutable `skill_drafts`, and immutable
`skill_versions` in its own Postgres schema (named after the installed cassette
name), and runs its own migrations at startup. There is no `org_id`: tenancy is gateway-owned, and
attribution rides the gateway-trusted `x-paper-auth-subject` header.

Source transcripts for generation are read from the configured Tapes core over
its trace API (`GET /v1/traces?session_id=` and `GET /v1/traces/{id}`); the
cassette holds no core database credential and reads no contract views.
`sourceSessionIds` is server-authored generation lineage, copied into each
published version and never rewritten by generic draft edits. User-curated
session relationships are a separate product concern and are not overloaded
onto provenance.

## Run

```bash
make build-local
CASSETTE_CORE_URL=http://127.0.0.1:8081 \
  ./build/skills-cassette serve --listen 127.0.0.1:9999
curl http://127.0.0.1:9999/ping
curl http://127.0.0.1:9999/openapi
curl http://127.0.0.1:9999/api/skills
```

Then register it with a core:

```bash
tapes serve --cassettes=http://127.0.0.1:9999/openapi
curl http://localhost:8081/v1/cassettes/skills
```

Configuration arrives entirely through the environment supplied by the
deployment, following the manifest's config schema:

| Variable | Meaning |
| --- | --- |
| `CASSETTE_NAME` | Installed cassette name (default `skills`); drives the route prefix and schema |
| `CASSETTE_CORE_URL` | Optional Tapes core API origin for session-backed generation. `https` is required except for loopback and cluster-local Service targets (`*.svc`, `*.svc.cluster.local`); brief-only generation works without it |
| `CASSETTE_LLM_PROVIDER` | `openai` (default), `anthropic`, or `ollama` |
| `CASSETTE_LLM_MODEL` | Model override; each provider has a sensible default |
| `CASSETTE_LLM_API_KEY` | Provider API key (falls back to `OPENAI_API_KEY` / `ANTHROPIC_API_KEY`) |
| `CASSETTE_LLM_BASE_URL` | Provider base URL override |
| `TAPES_DATABASE_URL` | Postgres DSN. Without one the cassette runs on a non-durable in-memory store |

## Remaining cutover work

- [ ] Migrate existing skills, versions, authorship, provenance, and download counts from Tapes core with rollback coverage.
- [ ] Route legacy `/v1/skills*` traffic to this cassette before deleting the implementation from Tapes core.
- [ ] Provision the cassette role, secrets, configuration, NetworkPolicies, and HTTPRoutes through TKO.
- [ ] Verify local-router and Envoy route precedence, subject-header trust, fresh installs, upgrades, and rollback.
- [ ] Replace the pinned trace-wire compatibility fixtures with fixtures generated from the core's published OpenAPI contract.

## Develop

Use the pinned Nix/Dagger environment when available:

```bash
nix develop
make format
make test
make build-local
make check
```

Run `make help` for the complete build and release surface.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution
intentionally submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.

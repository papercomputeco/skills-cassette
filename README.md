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

Every route the pre-cutover Tapes core served under `/v1/skills` exists here
1:1 under the cassette prefix:

| Cassette-local route | Public route (through core) | Purpose |
| --- | --- | --- |
| `GET /api/skills` | `GET /v1/cassettes/skills` | Keyset-paginated list with search (`q`), scopes (`scope=all\|mine\|team`), sort (`sort=downloads`), and per-tab counts |
| `GET /api/skills?session_id=` | `GET /v1/cassettes/skills?session_id=` | Provenance reverse lookup, replacing legacy `GET /v1/sessions/:id/skills` |
| `POST /api/skills` | `POST /v1/cassettes/skills` | Create an authored-from-scratch skill |
| `POST /api/skills/generate` | `POST /v1/cassettes/skills/generate` | LLM generation from nominated source sessions |
| `GET /api/skills/{id}` | `GET /v1/cassettes/skills/{id}` | Point read by opaque id |
| `PUT /api/skills/{id}` | `PUT /v1/cassettes/skills/{id}` | Partial update of the editable head |
| `DELETE /api/skills/{id}` | `DELETE /v1/cassettes/skills/{id}` | Creator-gated delete, history included |
| `GET /api/skills/{id}/skill.md` | `GET /v1/cassettes/skills/{id}/skill.md` | Drop-in SKILL.md download (counts a download) |
| `GET /api/skills/{id}/versions` | `GET /v1/cassettes/skills/{id}/versions` | Full published history |
| `POST /api/skills/{id}/versions` | `POST /v1/cassettes/skills/{id}/versions` | Publish an immutable version snapshot |
| `POST /api/skills/{id}/duplicate` | `POST /v1/cassettes/skills/{id}/duplicate` | Fork under a fresh id |

`/ping` and `/openapi` are the process anchors core probes and fetches; they
are not part of the proxied API.

The cassette owns two tables, `skills` and `skill_versions`, in its own
Postgres schema (named after the installed cassette name), and runs its own
migrations at startup. There is no `org_id`: tenancy is gateway-owned, and
attribution rides the gateway-trusted `x-paper-auth-subject` header.

Source transcripts for generation are read from the configured Tapes core over
its trace API (`GET /v1/traces?session_id=` and `GET /v1/traces/{id}`); the
cassette holds no core database credential and reads no contract views.

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
| `CASSETTE_CORE_URL` | Tapes core API origin for reading trace transcripts. `https` is required for non-loopback targets; unset disables generation (501) |
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
- [ ] Decide hard transcript and provenance limits for generation.

## Install a released build

```bash
curl -sSfL https://download.tapes.dev/skills-cassette/install | bash
```

Set `SKILLS_CASSETTE_VERSION` to select a release or nightly build and
`SKILLS_CASSETTE_INSTALL_DIR` to override `/usr/local/bin`.

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

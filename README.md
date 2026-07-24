# skills-cassette

!!! Warning - in flight transition from `tapes` core. Still needed:

**Prerequisites**

- [ ] Land gateway-owned tenancy/auth and remove `org_id` from the final skills schema and keys.
- [ ] Consume the Track 4 cassette manifest, discovery, API conventions, role grants, and router contracts once published; do not invent a local manifest format.
- [ ] Publish an authoritative core OpenAPI contract for the trace endpoints and replace the pinned JSON compatibility fixtures with generated contract fixtures.
- [ ] Provision cassette roles, secrets, configuration, NetworkPolicies, and HTTPRoutes through TKO.

**Cassette implementation**

- [ ] Add configured OpenAI, Anthropic, and Ollama adapters without importing Tapes credential state.
- [ ] Re-home the four historical migration steps as the final two physical tables (`skills` and `skill_versions`), without core foreign keys.
- [ ] Port storage and the production skills API only after the prerequisite contracts land.
- [ ] Support `GET /v1/skills?session_id=` and define compatibility for legacy `GET /v1/sessions/:id/skills` callers.
- [ ] Publish the skills OpenAPI operations and generated clients after route ownership is settled.
- [ ] Validate generated name, description, and content; decide hard transcript and provenance limits.
- [ ] Validate the configured core URL and require TLS for non-loopback targets.
- [ ] Keep generation request-driven; skills has no worker.

**Cutover**

- [ ] Migrate existing skills, versions, authorship, provenance, and download counts with rollback coverage.
- [ ] Route legacy `/v1/skills*` traffic to this cassette before deleting the implementation from Tapes core.
- [ ] Verify local-router and Envoy route precedence, subject-header trust, fresh installs, upgrades, and rollback.
- [ ] Complete operator, migration, API, and release documentation.

---

`skills-cassette` extracts reusable `SKILL.md` content from Tapes trace data.

This repository is **pre-cutover**. The current milestone owns the portable
skill-generation kernel, a focused HTTP boundary to Tapes core
(`GET /v1/traces?session_id=` and `GET /v1/traces/:id`), and a dark health
server. It does not yet expose production skill routes, own a database schema,
or run a worker.

## Run the health server

```bash
make build-local
./build/skills-cassette serve --listen 127.0.0.1:8080
curl http://127.0.0.1:8080/ping
```

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

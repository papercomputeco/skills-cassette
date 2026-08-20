---
title: Deploying
description: The image, its configuration, the LLM providers it supports, and the storage it owns.
sidebar:
  order: 3
---

Tapes does not start cassettes. A deployment starts the process, supplies its
configuration and credentials, and tells tapes where to find its OpenAPI document.

```text
public.ecr.aws/g4e5l3z3/papercomputeco/skills-cassette:<release-tag>
```

The image tag is the release tag verbatim, `v` and all — copy one from
[releases](https://github.com/papercomputeco/skills-cassette/releases) and use it
as written, e.g. `…/skills-cassette:v0.3.0`. Pin a release; `nightly` is published
but is not one.

The version is not a number anyone maintains. A release stamps the tag it is
publishing at link time, and the manifest version, the image reference the
manifest advertises, and the OpenAPI info block all derive from it. A source
build reports `0.0.0`, because a source tree is not a release and a
plausible-looking number there would describe one that never happened.

It listens on `9998` by default and serves `/ping`, `/openapi`, and its API under
`/api/skills`.

## Configuration

Everything arrives through the environment, following the manifest's config schema.

| Variable | Meaning |
| --- | --- |
| `CASSETTE_NAME` | Installed name (default `skills`). Drives the route prefix, schema and role. |
| `CASSETTE_CORE_URL` | Tapes core API origin for reading trace transcripts. **Unset disables generation** (`501`). |
| `CASSETTE_LLM_PROVIDER` | `openai` (default), `anthropic`, or `ollama`. |
| `CASSETTE_LLM_MODEL` | Model override. Each provider has a default. |
| `CASSETTE_LLM_API_KEY` | Provider API key. Falls back to `OPENAI_API_KEY` / `ANTHROPIC_API_KEY`. |
| `CASSETTE_LLM_BASE_URL` | Provider base URL override, for proxies and self-hosted endpoints. |
| `TAPES_DATABASE_URL` | Postgres DSN. |

`CASSETTE_CORE_URL` **requires `https`**, except for loopback and cluster-local
Service targets (`*.svc`, `*.svc.cluster.local`). Transcripts are session content;
the exception exists for traffic that never leaves the cluster, not as a general
opt-out.

Without an LLM key, generation answers `422` rather than failing at startup — the
rest of the API keeps working, so a deployment without a provider is still a usable
skills store.

## Storage

```toml
[[tables]]
name = "skills"

[[tables]]
name = "skill_versions"
```

The cassette owns those two tables in its own Postgres schema, named after the
installed cassette name, and runs its own migrations at startup. Core neither
creates the schema nor grants access to it.

`depends.views` is empty. This cassette reads no contract views and holds no core
database credential — transcripts arrive over HTTP. The only database it touches is
its own.

**Without `TAPES_DATABASE_URL` the cassette runs on a non-durable in-memory store.**
It will start, serve, generate and version quite happily, and lose everything on
restart. That is fine for a demo and silently wrong for anything else, because
nothing about the running system announces it.

## Pointing tapes at it

Tapes needs the exact URL of the metadata-bearing OpenAPI document:

```bash
tapes serve --cassettes=http://127.0.0.1:9998/openapi
curl http://localhost:8081/v1/cassettes/skills
```

or in `.tapes/config.toml`:

```toml
cassettes = ["http://127.0.0.1:9998/openapi"]
```

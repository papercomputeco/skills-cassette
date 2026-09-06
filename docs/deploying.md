---
title: Deploying
description: Image identity, tenant-local dependencies, worker configuration, and durable storage.
sidebar:
  order: 3
---

> Storage and migration details below describe the target unified-revision
> lifecycle in `docs/features/0001-durable-skill-generation`.

Tapes does not start cassettes. A deployment starts the process, supplies its
configuration and credentials, and tells Tapes where to fetch its OpenAPI
document.

```text
public.ecr.aws/g4e5l3z3/papercomputeco/skills-cassette:<release-tag>
```

The image tag is the release tag verbatim, `v` and all. Pin a tag from
[releases](https://github.com/papercomputeco/skills-cassette/releases), such as
`…/skills-cassette:v0.3.0`; `nightly` is published but is not a release.

A release stamps its tag into the binary. The manifest version, advertised image
reference, and OpenAPI info version all derive from that value. A source build
reports `0.0.0`, which deliberately cannot be mistaken for a release.

The process listens on `9998` by default and serves `/ping`, `/openapi`,
`/metrics`, and its product API under `/api/skills`.

## Configuration

Configuration arrives through environment variables corresponding to the
manifest schema.

| Variable | Meaning |
| --- | --- |
| `CASSETTE_NAME` | Installed name (default `skills`); drives route prefix, Postgres schema, and role. |
| `CASSETTE_CORE_URL` | Tenant-local Tapes core origin for trace reads and skills-evaluator calls. |
| `CASSETTE_LLM_PROVIDER` | `openai` (default), `anthropic`, or `ollama`. |
| `CASSETTE_LLM_MODEL` | Model override; each provider has a default. |
| `CASSETTE_LLM_API_KEY` | Provider API key; falls back to `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`. |
| `CASSETTE_LLM_BASE_URL` | Provider base URL override for proxies or self-hosted endpoints. |
| `CASSETTE_GENERATION_MAX_SESSIONS` | Selected-session limit for one generation (default 8, range 1–100). |
| `CASSETTE_GENERATION_CANDIDATE_CONCURRENCY` | Candidate inference concurrency (default 2, capped by max sessions). |
| `CASSETTE_GENERATION_MAX_TRANSCRIPT_BYTES` | Per-session rendered transcript limit (default 1048576, range 4096–1048576). |
| `CASSETTE_FILTERS` | Optional external attachment-view filter definitions as JSON. |
| `TAPES_DATABASE_URL` | Postgres DSN for durable state and worker coordination. |

`CASSETTE_CORE_URL` must use HTTPS except for loopback and Kubernetes
cluster-local Service targets (`*.svc` and `*.svc.cluster.local`). Session
content crosses this connection, so plaintext is only accepted for traffic that
does not leave the host or cluster.

The same tenant-local core URL is used for candidate evaluation at
`/v1/cassettes/skills-evaluator/candidate-evaluations`. Skills-cassette sends
the immutable criteria snapshot stored on the generation; the evaluator must
support stateless execution of caller-owned profile/version/criteria inputs.
Asynchronous generation requires all of the following:

- a configured and reachable Tapes core;
- a configured LLM provider (and key unless the provider does not require one);
- a reachable skills-evaluator cassette aggregated by that core;
- revision and generation persistence, normally Postgres.

If any worker dependency is absent at startup, revision reads and manual appends
still serve, but the generation worker is disabled. A durable generation can
remain queued until a correctly configured replica claims it. There is no
synchronous fallback and no production mock evaluator.

## Worker model

Generation execution is self-contained in skills-cassette. Replicas poll durable
Postgres state, claim work with expiring leases, heartbeat while processing, and
use a claim token to fence every persisted stage. Retry count, backoff,
concurrency, transcript size, candidate count, evaluator payloads, diagnostics,
and graceful drain are bounded.

The database is the lifecycle source of truth. A crash or lease expiry may repeat
external work, but deterministic artifact identities and unique storage
constraints prevent duplicate candidates, evaluations, and result revisions.
The winning private revision is appended in the same fenced transaction that
marks a generation complete. No NATS subject, external queue, or separate worker
deployment is required in v1.

`/metrics` exposes fixed-cardinality queue, lag, stage, duration, claim, partial
failure, revision append, visibility, latest, and worker health signals. IDs,
creator subjects, transcripts, and raw provider errors are never metric labels.

## Storage

The unified model owns these logical tables in its installed-name schema:

```text
skills
skill_revisions
skill_revision_visibility
skill_generations
generation_sessions
generation_candidates
candidate_evaluations
generation_diagnostics
revision_evaluations
```

`skills.latest_revision_id` is nullable and references `skill_revisions.id`.
Revision content/provenance rows are immutable; visibility/audit metadata is
separate. `skills.next_revision_sequence` (or an equivalent row lock) allocates
per-skill sequence numbers in the same transaction that inserts a revision.
Normal APIs expose no saved-revision deletion.

The migration retains both historical shapes: published versions become public
revisions, the published head becomes explicit latest, draft checkpoints become
creator-private revisions, and a differing mutable working copy receives one
retained revision. That retained copy is private to its author when the
predecessor row recorded one; a working copy with no recorded author was already
readable by the whole organization, so it migrates as a public revision rather
than as a private revision nobody could ever read. Latest stays on the published
head in both cases. Evaluation-linked identities are preserved where possible;
all other rows are mapped by explicit migration tables. After backfill,
constraints reject dangling latest, lineage, generation-result, and evaluation
references before predecessor columns are retired.

Core creates none of these tables. `depends.views` is empty because transcripts
arrive over HTTP. Optional external-filter views are deployment-supplied through
`CASSETTE_FILTERS`; their `SELECT` grants are also deployment-owned. A configured
view is probed before its parameter arms, re-probed in the background while
unreadable, and fails closed with `503` if it breaks after arming.

Without `TAPES_DATABASE_URL`, the cassette uses a non-durable in-memory store.
That supports local development but loses skill identities, revisions,
generations, and artifacts on restart. It must not be used where recovery or
history matters.

## Pointing Tapes at the cassette

Tapes needs the exact URL of the metadata-bearing OpenAPI document:

```bash
tapes serve --cassettes=http://127.0.0.1:9998/openapi
curl http://localhost:8081/v1/cassettes/skills
```

or in `.tapes/config.toml`:

```toml
cassettes = ["http://127.0.0.1:9998/openapi"]
```

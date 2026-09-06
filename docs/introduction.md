---
title: Skills cassette
description: Stable skills, immutable private/public revisions, exact evaluation, and durable asynchronous generation.
sidebar:
  order: 1
---

> This page describes the target unified-revision lifecycle proposed in
> `docs/features/0001-durable-skill-generation`.

`skills-cassette` turns agent-session evidence and author guidance into reusable
`SKILL.md` documents without treating long model work as one synchronous HTTP
mutation. Skill identities, immutable revisions, visibility metadata, explicit
latest selection, generations, candidates, evaluator results, and diagnostics
are durable resources.

It is a [cassette](https://tapes.dev/docs/cassettes/)—an independently deployed
HTTP service that Tapes discovers from its OpenAPI document and reverse-proxies
under the Tapes namespace.

## Addresses

The cassette serves its API under a local prefix; Tapes republishes it under
`/v1/cassettes/<name>`:

| On the cassette listener | Through Tapes |
| --- | --- |
| `GET /api/skills` | `GET /v1/cassettes/skills` |
| `POST /api/skills/{skillId}/revisions` | `POST /v1/cassettes/skills/{skillId}/revisions` |
| `POST /api/skills/{skillId}/generations` | `POST /v1/cassettes/skills/{skillId}/generations` |
| `GET /api/skills/{skillId}/skill.md` | `GET /v1/cassettes/skills/{skillId}/skill.md` |

Clients use the Tapes address. The local address separates cassette behavior
from proxying. `/ping`, `/openapi`, and `/metrics` remain outside the product
prefix because Tapes probes or fetches them directly.

## Unified revision lifecycle

A **skill** is one stable organization-scoped identity with a unique normalized
slug. It may temporarily exist with zero revisions while editing or generation
is in progress, but remains hidden.

A **revision** is a complete immutable skill snapshot with a globally unique
UUID, creator, optional base/source revision, content hash, optional change note,
and immutable per-skill sequence number. V1 displays that sequence as the version
string. Every evaluation, generation result, latest pointer, and lineage record
uses the revision UUID rather than the version label.

A private revision is visible only to its creator and replaces the old draft
resource. A public revision is visible to other organization members. Public is
an organization-sharing boundary, not internet publication. Visibility may
change without changing revision content.

A skill may have an explicit public **latest** revision. When present it remains
pinned until moved or cleared. Without it, unversioned consumers use the newest
public revision by sequence. A private revision cannot be latest, and an
explicitly latest revision must be moved or cleared before becoming private.
Only the explicit pointer produces a Latest badge.

There is no durable mutable working copy. Console edits locally and appends only
when the user selects **Save new revision**. Starting from any accessible
revision is allowed, and concurrent saves create independent revisions rather
than conflicts.

## Asynchronous generation

A generation captures the stable skill, optional base revision, complete input,
author context, ordered selected sessions, creator, and one immutable
skills-cassette-owned ranking rubric before returning `202`.

A cassette-local leased worker creates one complete candidate per selected
session without combining raw transcripts. Context-only generation creates one
candidate without a session. Skills-evaluator executes the same caller-owned
criteria for every candidate; skills-cassette retains bounded judgments and
selects deterministically. Partial source failures do not discard healthy work.

Many generations may run for one skill concurrently. Successful completion
appends the selected result exactly once as a new private revision. Generation
never changes visibility or latest, and there is no working-copy reconciliation
or `awaiting_input` state. Cancellation fences late writes while retaining
history.

Generation candidates, rubric, ranking, bounded diagnostics, and source
references become organization-visible if the result revision is made public.
Raw transcripts and raw provider/model errors are never embedded in that record.

## Evaluation boundary

Stateless generation-candidate judgment uses
`/v1/cassettes/skills-evaluator/candidate-evaluations`. Skills-cassette owns and
snapshots ranking policy; skills-evaluator owns transcript triage, judge
execution, normalization, and weighted scoring.

Durable user-requested evaluation targets one exact accessible revision UUID,
including private revisions. Evaluation history follows that revision's current
visibility, so private work and its findings remain creator-only.

Source transcripts come from the configured tenant-local Tapes core over its
trace API and remain transient bounded inputs. There is no `org_id` column in
cassette storage: tenancy is gateway-owned, while the trusted
`x-paper-auth-subject` identifies revision creators and private access.

## Deliberately deferred

V1 has no arbitrary revision tags, revision deletion, or fine-grained release
authorization. Existing customer labels remain a separate namespace. Any
organization member may append from accessible content and operate public/latest
metadata under the current “we are all friends” tenant model.

There is also no synchronous generation, draft CRUD/autosave/checkpoint,
publication-copy, direct content mutation, or generation-resolution surface.

## Next

- [API reference](./api.md)—revision, visibility, latest, generation, and error contracts.
- [Deploying](./deploying.md)—image, migration, worker dependencies, and durable storage.

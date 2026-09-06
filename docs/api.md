---
title: API reference
description: Stable skills, immutable revisions, visibility, latest resolution, and asynchronous generation.
sidebar:
  order: 2
---

> This page describes the target unified-revision contract in
> `docs/features/0001-durable-skill-generation`. Until its implementation plan is
> complete, the branch may still contain predecessor draft/publication handlers.

Paths below are on the cassette's own listener. Through Tapes, replace
`/api/skills` with `/v1/cassettes/skills`.

## Resource model

A skill is a stable organization-local identity with a unique normalized slug.
It may temporarily contain no revisions. A revision is a complete immutable
snapshot with:

- globally unique `id`;
- per-skill immutable `sequenceNumber`;
- v1 version string derived from that sequence;
- creator and optional base/source revision UUIDs;
- complete content, hash, provenance, optional change note, and creation time.

Revision content never changes. `isPublic` and the skill's optional explicit
`latestRevisionId` are mutable metadata. Public means visible to members of the
same organization; private means creator-only.

Every foreign relationship uses revision UUID. Version and sequence are display
and ordering fields, not identity.

## Routes

| Route | Purpose |
| --- | --- |
| `POST /api/skills` | Create or resolve the hidden stable identity for a normalized slug |
| `GET /api/skills` | Paginated list with one effective entry per accessible skill |
| `GET /api/skills/{skillId}` | Read identity, effective public revision, and viewer-private continuation metadata |
| `GET /api/skills/{skillId}/skill.md` | Download the effective public revision as `SKILL.md` |
| `POST /api/skills/{skillId}/revisions` | Append a complete immutable private revision |
| `GET /api/skills/{skillId}/revisions` | List public plus viewer-private revision history |
| `GET /api/skills/{skillId}/revisions/{revisionId}` | Read one exact accessible revision |
| `PUT /api/skills/{skillId}/revisions/{revisionId}/visibility` | Make a revision public or private without changing content |
| `PUT /api/skills/{skillId}/latest` | Set or move the explicit latest pointer |
| `DELETE /api/skills/{skillId}/latest` | Clear the explicit latest pointer |
| `POST /api/skills/{skillId}/generations` | Durably queue independent asynchronous generation |
| `GET /api/skills/{skillId}/generations` | List accessible generation history for a skill |
| `GET /api/skills/{skillId}/generations/{generationId}` | Read persisted progress and artifacts |
| `GET /api/skills/generations/{generationId}` | Resolve generation directly by public ID |
| `POST /api/skills/{skillId}/generations/{generationId}/cancellations` | Cooperatively cancel and fence a generation |

Duplicating a revision into another skill is not a separate route: resolve the
target slug with `POST /api/skills`, read the source revision, then append its
snapshot to the target with `sourceRevisionId` naming that source. `SKILL.md` is served only for a skill's
effective public revision; an exact revision's content is read as JSON.

There is no mutable draft, autosave/checkpoint, publication-copy, generation
resolution, revision deletion, synchronous generation, exact-revision Markdown,
dedicated duplicate, or direct content-update route. V1 also exposes no arbitrary revision-tag operations; existing labels are
a separate namespace.

## Authentication and visibility

The tenant-local gateway establishes organization membership and supplies trusted
`x-paper-auth-subject`. Creator identity is never accepted in request JSON.

Storage applies access before loading content:

```text
revision.isPublic OR revision.creatorSubject == callerSubject
```

Public is organization-visible, not internet-wide. Any organization member may
append from an accessible revision and may change public/latest metadata in v1.
A guessed private revision ID behaves as not found.

Evaluation history follows its exact target revision's current visibility.
Generation history is creator-only until it has a result revision; when that
revision is public, bounded candidates/ranking/provenance become organization-
visible with it.

## Skill identity

Create or resolve a stable identity:

```json
{
  "slug": "diagnose-flaky-tests"
}
```

The slug is normalized and organization-unique. A collision returns the existing
skill ID but no inaccessible private content. A zero-revision identity is hidden
from ordinary listing.

## Effective list and detail reads

Unversioned reads choose:

```text
explicit public latest revision
  else highest-sequence public revision
  else no organization-visible default
```

`GET /api/skills` returns one entry per skill, never one per public revision. A
creator also sees private-only skills using their newest private revision. When a
public default exists, it remains the card/detail default even if the viewer has
newer private work; `newestPrivateRevision` and `hasNewerPrivateRevision` support
**Continue your private revision**.

Only equality with stored `latestRevisionId` sets `isLatest: true`. The automatic
newest-public fallback does not receive an inferred Latest marker.

Listing keeps the existing bounded keyset/search/filter shape:

| Parameter | Meaning |
| --- | --- |
| `limit` | Page size; bounded by the server maximum. |
| `cursor` | Opaque keyset cursor from `nextCursor`. A cursor records the `sort` it was cut under and is rejected with 400 `invalid_request` when replayed under another `sort`; cursors issued by replicas that recorded no sort are accepted under either, so paging keeps working across a rolling deployment. |
| `q` | Search accessible effective name, description, and labels. |
| `scope` | `all`, `mine`, or `team` under creator/public visibility. |
| `sort` | `downloads` or the default effective-update order. |
| `session_id` | Return accessible skills attributed to a source session. |

Configured external attachment filters remain fail-closed after arming.

## Append a revision

```json
{
  "basedOnRevisionId": "revision-uuid-or-null",
  "snapshot": {
    "slug": "diagnose-flaky-tests",
    "name": "Diagnose flaky tests",
    "description": "Use when a test fails intermittently.",
    "type": "workflow",
    "tags": ["testing"],
    "content": "# Diagnose flaky tests\n...",
    "isAiGenerated": false,
    "sourceSessionIds": []
  },
  "changeNote": "Add a reproducibility check",
  "idempotencyKey": "editor-save-018"
}
```

The server assigns revision UUID, sequence, version, creator, timestamps, and
private visibility. `basedOnRevisionId` may name any accessible revision in the
same skill. Concurrent saves from one base all succeed with distinct sequences.
The original remains unchanged.

`sourceRevisionId` is used instead of `basedOnRevisionId` for a cross-skill
duplicate. The server does not copy anything: the client supplies the complete
`snapshot` exactly as for any append (typically the snapshot it just read from
the source revision), `sourceRevisionId` must name a revision of another skill
that is accessible to the caller, and the new private revision records that
source as lineage with origin `duplicate`. Repeating an idempotency key returns
the retained append.

## Visibility

```json
{ "isPublic": true }
```

`PUT /visibility` updates only visibility/audit metadata. Repeating the requested
state is successful. Making the explicitly latest revision private returns a
conflict until latest is moved or cleared.

Normal user APIs do not delete revisions. Making a revision private hides it from
other organization members and hides its linked evaluation/generation history.

## Explicit latest

Set or move:

```json
{ "revisionId": "revision-uuid" }
```

The target must be a public revision of the same skill. The pointer remains
pinned when newer revisions become public. `DELETE /latest` clears the override
and restores automatic newest-public resolution. Setting or clearing does not
change content or visibility.

## Asynchronous generation

```json
{
  "baseRevisionId": "revision-uuid-or-null",
  "input": {
    "name": "Diagnose flaky tests",
    "description": "Turn session evidence into a reusable workflow.",
    "type": "workflow",
    "tags": ["testing"]
  },
  "authorContext": "Prefer reproducible evidence.",
  "selectedSessionIds": ["session-a", "session-b"]
}
```

A successful create returns `202` only after inputs and the immutable
skills-cassette-owned ranking rubric are durable. Multiple generations for one
skill/base may run concurrently. Statuses are:

```text
queued -> generating_candidates -> evaluating_candidates -> synthesizing
       -> completed | failed | canceled
```

There is no `awaiting_input` or resolution. Each selected session produces one
independent candidate inference; context-only generation produces one candidate
without a transcript. Partial source failures retain bounded diagnostics while
healthy candidates continue.

Every viable candidate and optional synthesis is judged under the same profile,
version, weighted criteria, author context, and explicit evidence ordering by
skills-evaluator. Skills-cassette deterministically ranks persisted results.

Successful completion appends exactly one new private revision and returns its
`resultRevisionId`. It never makes that revision public or latest. Cancellation
retains artifacts and fences late worker writes.

## Errors

Lifecycle failures use a bounded stable envelope:

```json
{
  "error": {
    "code": "revision_not_public",
    "message": "Only a public revision can be marked latest.",
    "resourceId": "revision-uuid"
  }
}
```

Codes include `revision_not_found`, `revision_private`, `revision_not_public`,
`latest_revision_conflict`, `revision_sequence_conflict`, and
`invalid_generation_state`. Inaccessible private content uses not-found
semantics. Responses never include raw transcripts, provider/model errors,
creator subjects, claim tokens, or lease state.

## Composed Console workflow

Console may present **Save new revision**, **Make public**, and **Make latest**
together, but calls append → visibility → latest as separate operations. Make
latest selects and locks Make public. If a later operation fails, earlier success
remains committed and the client retries only the failed step.

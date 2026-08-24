---
title: API reference
description: Every skills route — listing, generation, the editable head, published versions, and the SKILL.md download.
sidebar:
  order: 2
---

Paths below are given on the cassette's own listener. Through tapes, replace
`/api/skills` with `/v1/cassettes/skills`.

| Route | Purpose |
| --- | --- |
| `GET /api/skills` | Paginated list, with search, scopes and sort |
| `POST /api/skills` | Create a skill authored from scratch |
| `POST /api/skills/generate` | Generate a skill from nominated sessions |
| `GET /api/skills/{id}` | Read one skill |
| `PUT /api/skills/{id}` | Update the editable head |
| `DELETE /api/skills/{id}` | Delete, history included |
| `GET /api/skills/{id}/skill.md` | Download the SKILL.md |
| `GET /api/skills/{id}/versions` | Published history |
| `POST /api/skills/{id}/versions` | Publish a version snapshot |
| `POST /api/skills/{id}/duplicate` | Fork under a fresh id |

## Listing

`GET /api/skills`

| Parameter | Meaning |
| --- | --- |
| `limit` | Page size. Default 24, max 100. |
| `cursor` | Opaque keyset cursor from a previous `next_cursor`. |
| `q` | Search over name, description and tags. |
| `scope` | Which slice: `all`, `mine`, or `team`. |
| `sort` | `downloads` for most-downloaded; defaults to most recently updated. |
| `session_id` | Return only skills generated from this session. Unpaginated. |

Pagination mirrors the tapes read API: pass the returned `next_cursor` to continue,
and its absence means the last page. **Reset the cursor when changing `sort`** — a
keyset cursor encodes a position in one ordering and means nothing in another.

`session_id` switches modes rather than filtering: it answers "what came out of this
session?" and returns the whole answer without paging.

`400` on a malformed cursor, `500` on a listing failure.

## Generation

`POST /api/skills/generate`

Takes the session ids to learn from and generates a skill with the configured LLM.

| Code | When |
| --- | --- |
| `201` | The generated skill. |
| `400` | Invalid body, or `sessionIds` missing or empty. |
| `404` | One or more source sessions were not found. |
| `422` | The sources carried nothing the generator could use, or no LLM key is configured. |
| `500` | Generation or persistence failed. |
| `501` | No core URL is configured, so transcripts cannot be read at all. |

The distinction between `404`, `422` and `501` is the useful one. `404` means the
sessions do not exist; `422` means they do but yielded nothing worth a skill (or the
provider is unusable); `501` means this cassette was never told where tapes is. Only
the last is a deployment error — see [Deploying](./deploying.md).

## The editable head

`POST /api/skills` creates one from scratch: `201`, or `400` on an invalid body.

`GET /api/skills/{id}` reads one: `200`, or `404`.

`PUT /api/skills/{id}` updates the head: `200`, `400` on an invalid body, `404`,
or **`403` when the skill has a creator and the caller is not that creator**.

`DELETE /api/skills/{id}` deletes the skill and its history: `204`, `404`, or
`403` under the same owner rule. A skill with no recorded author is unattributed
and may be mutated by anyone.

The `PUT`, `DELETE`, and publish routes enforce this owner rule from the
gateway-trusted `x-paper-auth-subject` header.

`POST /api/skills/{id}/duplicate` forks under a fresh id: `201`, or `404`.

## Versions

`GET /api/skills/{id}/versions` lists the published history: `200`, `500`.

`POST /api/skills/{id}/versions` publishes an immutable snapshot: `201`, `200`
for an idempotent conditional retry, `400`, `403`, `404`, `409`, or `500`. The
version row and the head bump land together, so a publish never leaves a durable
snapshot behind a stale head.

Set `expectedContent` to the head used to prepare the new `content`. Publication
then compare-and-swaps the head: a different current head returns `409`. If the
same conditional request committed but its acknowledgement was lost, retrying its
identical `content`, `changelog`, and `expectedContent` returns the existing
version with `200` instead of minting a duplicate. Without `expectedContent`, callers retain the legacy unconditional
publish behavior and should inspect version history before retrying.

Concurrent publishes racing for the same number are resolved internally; a
conditional publish instead returns `409` if another writer changed its expected
head.

## Download

`GET /api/skills/{id}/skill.md` returns `text/markdown` — the drop-in document.
`200` or `404`.

This route counts a download, which is what `sort=downloads` ranks. Anything
polling it inflates that ordering.

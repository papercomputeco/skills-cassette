---
title: Skills cassette
description: Generates, stores, versions, and serves reusable SKILL.md documents extracted from tapes sessions.
sidebar:
  order: 1
---

`skills-cassette` turns what an agent actually did into something reusable. It reads
the transcripts of sessions you nominate, generates a `SKILL.md` from them with an
LLM, and then stores, versions and serves that document — so a skill learned once
can be dropped into the next agent that needs it.

It is a [cassette](https://tapes.dev/docs/cassettes/) — an independently deployed
HTTP service that tapes discovers from its OpenAPI document and reverse-proxies
under the tapes namespace.

## The two addresses

The cassette serves its API under a local prefix on its own listener; tapes
republishes that API under `/v1/cassettes/<name>`:

| On the cassette's own listener | Through tapes |
| --- | --- |
| `GET /api/skills` | `GET /v1/cassettes/skills` |
| `POST /api/skills/generate` | `POST /v1/cassettes/skills/generate` |
| `GET /api/skills/{id}/skill.md` | `GET /v1/cassettes/skills/{id}/skill.md` |

Clients use the tapes address. The local one is what tapes itself talks to, and what
you curl to tell a cassette problem apart from a proxying problem — the cassette
does not know tapes exists.

`/ping` and `/openapi` sit outside the prefix. They are the anchors tapes probes and
fetches, not part of the proxied API.

## How it reads sessions

Source transcripts come from the configured tapes core **over its trace API** —
`GET /v1/traces?session_id=` and `GET /v1/traces/{id}` — not from the database.

That is a deliberate boundary: this cassette holds no core database credential and
declares no contract views. Its entire relationship with tapes is HTTP, so a
deployment that gets the database wiring wrong cannot break transcript reads.

**The trace client sends no credential** — no bearer token, no
`x-paper-auth-subject`. What authorizes those reads is where the cassette can reach
and what core accepts from it, so the network path and core's own configuration are
the whole of the control. Treat `CASSETTE_CORE_URL` as pointing at a core that is
willing to answer this cassette, and see [Deploying](./deploying.md) for the
transport requirement on it.

## Skills and versions

A skill has an editable head and an immutable published history. Editing the head
changes what the next reader gets; publishing snapshots it as a version that never
changes afterward. Downloads count against the skill, which is what the
`sort=downloads` ordering ranks.

There is no `org_id` here. Tenancy is gateway-owned, and attribution rides the
gateway-trusted `x-paper-auth-subject` header — the cassette trusts what the
gateway asserts and stores nothing about who else might exist.

## Next

- [API reference](./api.md) — every route, its parameters, and its status codes.
- [Deploying](./deploying.md) — image, configuration, providers, and storage.

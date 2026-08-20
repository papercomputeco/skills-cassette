---
title: Development
description: Build and test from a checkout, and what the test suite pins about the manifest.
sidebar:
  order: 4
---

Use the pinned Nix/Dagger environment when available:

```bash
nix develop
make format
make test
make build-local
make check
```

`make help` prints the complete build and release surface.

Run it against a local core:

```bash
make build-local
CASSETTE_CORE_URL=http://127.0.0.1:8081 \
  ./build/skills-cassette serve --listen 127.0.0.1:9999

curl http://127.0.0.1:9999/ping
curl http://127.0.0.1:9999/openapi
curl http://127.0.0.1:9999/api/skills
```

With no `TAPES_DATABASE_URL` this runs on the in-memory store, which is what you
want for a local loop and never what you want deployed — see
[Deploying](./deploying.md).

## What the tests pin

`cassette.toml` and the manifest embedded in `internal/server/openapi.go` are two
encodings of one schema. For the same installation identity their canonical manifest
digests must match, and the suite fails the build when they drift — so a cassette
cannot ship published metadata that disagrees with its authored metadata.

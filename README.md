# skills-cassette

Reusable `SKILL.md` documents for [tapes](https://tapes.dev): generated from
sessions you nominate, then stored, versioned and served — so a skill learned once
can be dropped into the next agent that needs it.

It is a [cassette](https://tapes.dev/docs/cassettes/) — an independently deployed
HTTP service that tapes discovers from its OpenAPI document and reverse-proxies
under `/v1/cassettes/skills`.

Source transcripts are read from the configured tapes core over its trace API, so
this cassette holds no core database credential and reads no contract views. Its
entire relationship with tapes is HTTP.

**Documentation:** [tapes.dev/docs/skills](https://tapes.dev/docs/skills/) — the API
reference, deployment and providers, and development.

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

Without `TAPES_DATABASE_URL` the cassette runs on a non-durable in-memory store —
fine for this loop, never for a deployment.

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

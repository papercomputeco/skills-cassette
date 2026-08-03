# Contributing

`skills-cassette` is the Tapes skills cassette: the skill-generation kernel
(`pkg/skill`), the cassette's own storage (`internal/storage`: `skills` and
`skill_versions` in the cassette's schema, plus an in-memory driver), and the
cassette HTTP service (`internal/server`: `/ping`, `/openapi` with the
embedded `cassette/v1alpha1` manifest, and the skills API under
`/api/<name>`).

Rules that keep the cassette admissible:

- Every API route must live under the declared local prefix (`/api/<name>`).
  A path outside the prefix fails the whole OpenAPI document at core
  admission.
- The `x-tapes-cassette` extension built in `internal/server/openapi.go` and
  the authored `cassette.toml` are two encodings of one manifest. Change them
  together; they must keep producing the same canonical manifest digest.
- Every OpenAPI operation needs a unique `operationId` and at least one
  response.
- The cassette reads Tapes only over its public trace API
  (`GET /v1/traces?session_id=`, `GET /v1/traces/{id}`). Do not add a core
  database dependency, core contract views, or Tapes credential state.
- There is no `org_id` and no worker: tenancy is gateway-owned
  (`x-paper-auth-subject`), and generation is request-driven.

Use the Nix flake dev shell, which pins Go and Dagger:

```bash
nix develop
make build-local
./build/skills-cassette --help
```

Always use Ginkgo/Gomega for tests. Use make operations for development and run:

```bash
make format
make test
make check
```

Do not write design documents or implementation plans to disk. Pull request
titles use the repository's accepted contribution labels, such as `✨ feat:`,
`🔧 fix:`, `🧹 chore:`, or `📚 docs:`.

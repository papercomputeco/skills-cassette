# Contributing

`skills-cassette` is currently a pre-cutover scaffold: it owns the portable
skill-generation kernel, the focused Tapes-core trace HTTP client, and a health
server only. Do not add production skill routes, schema ownership, migrations,
a cassette manifest, or a worker until the dependent cassette tracks land.

Use the Nix flake dev shell, which pins Go 1.25 and Dagger:

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

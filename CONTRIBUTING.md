# Contributing

Contributions are welcome! Whether it's a bug fix, new feature, documentation improvement, or just a question — we appreciate your help.

## Getting Started

```bash
git clone https://github.com/Neftedollar/Orleans.Streaming.Redis.git
cd Orleans.Streaming.Redis
dotnet build
dotnet test   # requires Docker (Testcontainers spins up Redis automatically)
```

> **Note:** Integration tests use [Testcontainers](https://dotnet.testcontainers.org/) — Docker must be running on your machine.

## Good First Issues

Look for issues labeled [`good first issue`](https://github.com/Neftedollar/Orleans.Streaming.Redis/labels/good%20first%20issue) — these are specifically chosen for new contributors.

## What We Need

### High Priority
- **Connection resilience** — Reconnection handling, circuit breaker on Redis failures
- **Benchmarks** — Throughput comparison with Azure Queue Streams and other providers

### Medium Priority
- **JSON payload mode** — Optional `System.Text.Json` serialization for interop with non-Orleans consumers
- **Connection pooling** — Multi-connection support for high-throughput scenarios
- **Orleans 8.x / 9.x compatibility testing** — CI runs on all three TFMs but more real-world testing is welcome

### Always Welcome
- Documentation improvements
- Additional test coverage
- Performance optimizations
- Bug reports with reproduction steps

## Development Workflow

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Ensure `dotnet build` and `dotnet test` pass
5. Submit a Pull Request

## Pull Request Guidelines

1. **One feature per PR** — keeps reviews focused
2. **Include tests** — unit tests for logic, integration tests for Redis interactions
3. **Update docs** if the public API changes (README + relevant docs/ files)
4. **Keep commits clean** — squash fixups before requesting review

## Code Style

- Follow existing conventions (`.editorconfig` is enforced)
- XML doc comments on all public types and members
- `internal` by default, `public` only for the API surface
- Use `System.Diagnostics.Metrics` for any new counters/gauges

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

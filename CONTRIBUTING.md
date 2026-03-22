# Contributing

Contributions are welcome! Here's how to help.

## Getting Started

```bash
git clone https://github.com/Neftedollar/Orleans.Streaming.Redis.git
cd Orleans.Streaming.Redis
dotnet build
dotnet test
```

## What We Need

### High Priority
- **`XACK` implementation** — `RedisStreamReceiver.MessagesDeliveredAsync` needs to call `XACK` with Redis entry IDs. This requires storing entry IDs in `RedisBatchContainer`.
- **Pending message recovery** — `XPENDING` / `XCLAIM` for messages that were read but never ACK'd (e.g., silo crash).
- **Integration tests** — Tests against a real Redis instance (Testcontainers).

### Medium Priority
- **Connection resilience** — Reconnection handling, circuit breaker on Redis failures.
- **Metrics** — Counters for messages enqueued/dequeued/failed via `System.Diagnostics.Metrics`.
- **Logging** — `ILogger` integration throughout the provider.

### Nice to Have
- **Orleans 8.x / 9.x compatibility testing** — The package targets `net8.0` and `net9.0` but is only tested on `net10.0`.
- **Benchmarks** — Throughput comparison with Azure Queue Streams.
- **NuGet CI/CD** — Automatic publishing on tag push.

## Pull Request Guidelines

1. One feature per PR
2. Include tests
3. Update README if the public API changes
4. `dotnet build` and `dotnet test` must pass

## Code Style

- Follow existing conventions (no `.editorconfig` enforced yet)
- XML doc comments on all public types
- `internal` by default, `public` only for the API surface

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

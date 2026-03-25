# Orleans.Streaming.Redis

[![CI](https://github.com/Neftedollar/Orleans.Streaming.Redis/actions/workflows/ci.yml/badge.svg)](https://github.com/Neftedollar/Orleans.Streaming.Redis/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Streaming.Redis.svg?logo=nuget)](https://www.nuget.org/packages/Orleans.Streaming.Redis)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Orleans.Streaming.Redis.svg?logo=nuget)](https://www.nuget.org/packages/Orleans.Streaming.Redis)
[![codecov](https://codecov.io/gh/Neftedollar/Orleans.Streaming.Redis/branch/main/graph/badge.svg)](https://codecov.io/gh/Neftedollar/Orleans.Streaming.Redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-512BD4?logo=dotnet)](https://dotnet.microsoft.com)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)
[![GitHub Stars](https://img.shields.io/github/stars/Neftedollar/Orleans.Streaming.Redis?style=social)](https://github.com/Neftedollar/Orleans.Streaming.Redis)

**Redis Streams persistent stream provider for [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/) 10.x**

Uses Redis Streams (`XADD` / `XREADGROUP` / `XACK`) as a durable, cross-silo message transport for Orleans streaming.

## Why?

Orleans ships with persistent stream providers for Azure Queue, Azure Event Hubs, and Amazon SQS — but **not Redis**. If you already use Redis for Orleans grain persistence and clustering, adding another infrastructure dependency just for streaming is unnecessary.

This package fills the gap: a lightweight Redis Streams adapter that works with any Redis 5.0+ instance.

## Features

- **Cross-silo delivery** — events published on Silo A are delivered to subscribers on Silo B
- **Consumer groups** — automatic offset tracking per silo via Redis consumer groups
- **Partitioned queues** — configurable number of stream partitions for parallelism
- **Bounded retention** — `MAXLEN` on `XADD` prevents unbounded stream growth
- **Multi-target** — supports `net8.0`, `net9.0`, `net10.0`
- **Minimal dependencies** — only `Microsoft.Orleans.Streaming` + `StackExchange.Redis`

## Quick Start

### Install

```bash
dotnet add package Orleans.Streaming.Redis
```

### Configure

```csharp
using Orleans.Streaming.Redis.Configuration;

builder.Host.UseOrleans(silo =>
{
    silo.AddRedisStreams("StreamProvider", options =>
    {
        options.ConnectionString = "localhost:6379";
        options.QueueCount = 8;           // number of partitions
        options.MaxStreamLength = 10_000; // MAXLEN per stream key
        options.KeyPrefix = "orleans:stream";
        options.ConsumerGroup = "orleans";
    });
});
```

### Produce

```csharp
var streamProvider = clusterClient.GetStreamProvider("StreamProvider");
var streamId = StreamId.Create("my-namespace", "my-key");
var stream = streamProvider.GetStream<MyEvent>(streamId);
await stream.OnNextAsync(new MyEvent { ... });
```

### Consume

```csharp
[ImplicitStreamSubscription("my-namespace")]
public class MyGrain : Grain, IAsyncObserver<MyEvent>
{
    public override async Task OnActivateAsync(CancellationToken ct)
    {
        var provider = this.GetStreamProvider("StreamProvider");
        var streamId = StreamId.Create("my-namespace", this.GetPrimaryKeyString());
        var stream = provider.GetStream<MyEvent>(streamId);
        await stream.SubscribeAsync(this);
    }

    public Task OnNextAsync(MyEvent item, StreamSequenceToken? token) { ... }
    public Task OnCompletedAsync() => Task.CompletedTask;
    public Task OnErrorAsync(Exception ex) => Task.CompletedTask;
}
```

## Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `ConnectionString` | `""` | Redis connection string (StackExchange.Redis format). **Required.** |
| `QueueCount` | `8` | Number of stream partitions. More = better parallelism. |
| `KeyPrefix` | `"orleans:stream"` | Redis key prefix. Stream keys: `{prefix}:{index}` |
| `ConsumerGroup` | `"orleans"` | Redis consumer group name. Each silo auto-joins. |
| `MaxBatchSize` | `100` | Max messages per `XREADGROUP` poll. |
| `MaxStreamLength` | `10000` | `MAXLEN` for `XADD`. Caps stream size. `0` = unlimited. |
| `Database` | `-1` | Redis database index. `-1` = default from connection string. |
| `CacheSize` | `4096` | In-memory cache capacity (batch containers) per queue partition. |
| `DeadLetterPrefix` | `null` | Redis key prefix for dead-letter stream. When set, undeserializable messages are forwarded here and XACK'd. `null` = disabled. |

## Architecture

```
Producer Grain
    ↓ stream.OnNextAsync(event)
RedisStreamAdapter.QueueMessageBatchAsync()
    ↓ XADD orleans:stream:{partition} * data <serialized>
Redis Stream (durable)
    ↓ XREADGROUP orleans silo-{guid} COUNT 100 > orleans:stream:{partition}
RedisStreamReceiver.GetQueueMessagesAsync()
    ↓ IBatchContainer
Orleans PullingAgent → IQueueCache → Subscriber Grain.OnNextAsync()
```

Each silo runs a pulling agent per queue partition. Messages are distributed across partitions via consistent hashing on `StreamId`.

## Redis Requirements

- **Redis 5.0+** (Streams support)
- Redis Cluster is supported (stream keys are partitioned by prefix)
- Recommended: enable `maxmemory-policy allkeys-lru` or set `MaxStreamLength` to prevent OOM

## Status

**v1.0.0** — Core functionality works, cross-silo delivery, consumer groups, crash recovery, dead-letter support.

### Roadmap

- [ ] Optional JSON payload mode for non-Orleans consumers (interop with Node.js, Python, Go, etc.)

## Documentation

- **[Getting Started](docs/getting-started.md)** — install, configure, produce, consume in 5 minutes
- **[Configuration Reference](docs/configuration.md)** — all options, pulling agent tuning, client setup, multiple providers
- **[Architecture](docs/architecture.md)** — data flow, components, serialization, crash recovery, metrics
- **[Production Deployment](docs/production.md)** — Redis sizing, HA, monitoring, scaling, Docker Compose
- **[Troubleshooting](docs/troubleshooting.md)** — common issues, Redis CLI debugging, logging

## Contributing

Contributions are welcome! Check out the [Contributing Guide](CONTRIBUTING.md) to get started.

- Browse [`good first issue`](https://github.com/Neftedollar/Orleans.Streaming.Redis/labels/good%20first%20issue) for beginner-friendly tasks
- Read the [Code of Conduct](CODE_OF_CONDUCT.md)
- Review the [Security Policy](SECURITY.md) for reporting vulnerabilities

## Community

- [GitHub Discussions](https://github.com/Neftedollar/Orleans.Streaming.Redis/discussions) — questions, ideas, show & tell
- [GitHub Issues](https://github.com/Neftedollar/Orleans.Streaming.Redis/issues) — bug reports and feature requests

If this project is useful to you, please consider giving it a :star: — it helps others discover it!

## License

[MIT](LICENSE) — free for commercial and open-source use.

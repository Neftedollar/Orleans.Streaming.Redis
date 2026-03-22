# Orleans.Streaming.Redis

[![NuGet](https://img.shields.io/nuget/v/Orleans.Streaming.Redis.svg)](https://www.nuget.org/packages/Orleans.Streaming.Redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

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
var stream = streamProvider.GetStream<MyEvent>("my-namespace", "my-key");
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
        var stream = provider.GetStream<MyEvent>("my-namespace", this.GetPrimaryKeyString());
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

**v0.1.0 — Alpha.** Core functionality works.

### v0.2.0 Roadmap

<!-- TODO(v0.2): Optional JSON serialization mode — allow configuring an alternative payload
     encoding (e.g. System.Text.Json) instead of the default Orleans binary serializer.
     Useful for interop with non-Orleans consumers that read from the same Redis Streams.
     Decided out of scope for v0.1 to avoid over-engineering; revisit when there is a
     concrete use-case. -->

- [ ] Optional JSON payload mode for non-Orleans consumers (v0.2, see TODO above)
- [ ] Connection pooling / multi-connection support for high-throughput scenarios

Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)

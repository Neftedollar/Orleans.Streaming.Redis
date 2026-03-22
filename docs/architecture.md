# Architecture

## Overview

Orleans.Streaming.Redis implements the [Orleans persistent stream provider](https://learn.microsoft.com/dotnet/orleans/streaming/stream-providers) contract using [Redis Streams](https://redis.io/docs/data-types/streams/) as the durable message transport.

```
┌─────────────────── Silo A ───────────────────┐    ┌─────────────────── Silo B ───────────────────┐
│                                               │    │                                               │
│  ProducerGrain                                │    │  SubscriberGrain                              │
│    │ stream.OnNextAsync(event)                 │    │    ▲ OnNextAsync(event, token)                │
│    ▼                                          │    │    │                                          │
│  RedisStreamAdapter                           │    │  PullingAgent (one per partition)              │
│    │ QueueMessageBatchAsync()                 │    │    │ GetQueueMessagesAsync()                   │
│    │ ┌─ serialize event → byte[] ─┐           │    │    │ ┌─ deserialize byte[] → event ─┐         │
│    │ └─ XADD stream:3 * data ─── │──────┐    │    │    │ └─ XREADGROUP group consumer ──│──┐      │
│    ▼                               │      │    │    │    ▼                               │  │      │
│  RedisStreamQueueMapper           │      │    │    │  RedisQueueCache                   │  │      │
│    │ StableHash(streamId) % N     │      │    │    │    │ AddToCache()                   │  │      │
│    └─ partition = 3               │      │    │    │    │ GetCacheCursor()               │  │      │
│                                   │      │    │    │    ▼                               │  │      │
└───────────────────────────────────┘      │    └────│  MessagesDeliveredAsync()          │  │      │
                                           │         │    │ XACK stream:3 group entryId   │  │      │
                                           │         └────│────────────────────────────────┘  │      │
                                           │              │                                  │      │
                                           ▼              ▼                                  │      │
                                    ┌─────────────── Redis ──────────────────────────────────┘      │
                                    │                                                               │
                                    │  stream:0  ──────  stream:1  ──────  stream:2  ──────  ...    │
                                    │  stream:3  ◄─── XADD ──── data={serialized} ─────────────────┘
                                    │                                                               │
                                    │  Consumer Group: "orleans"                                    │
                                    │    consumer: silo-a1b2c3...  (Silo A)                         │
                                    │    consumer: silo-d4e5f6...  (Silo B)                         │
                                    │                                                               │
                                    └───────────────────────────────────────────────────────────────┘
```

## Components

### RedisStreamAdapterFactory

- Implements `IQueueAdapterFactory`
- Creates `ConnectionMultiplexer` with reconnect policy (`AbortOnConnectFail=false`, `ExponentialRetry`)
- Validates `RedisStreamOptions` before connecting
- Registered via `AddRedisStreams()` extension method
- Lifecycle managed by `RedisStreamFactoryLifetimeService` (IHostedService)

### RedisStreamAdapter

- Implements `IQueueAdapter`
- **Producer side:** `QueueMessageBatchAsync()` → serialize events → `XADD`
- **Consumer side:** `CreateReceiver()` → one `RedisStreamReceiver` per partition
- Retry: 3 attempts with exponential backoff (100ms/200ms/400ms) on `XADD`
- Each event is individually serialized as `byte[]` with full Orleans type envelope

### RedisStreamReceiver

- Implements `IQueueAdapterReceiver`
- **Two-phase read** in `GetQueueMessagesAsync()`:
  1. Phase 1: `XREADGROUP ... "0"` — read pending (claimed but unacked) messages
  2. Phase 2: `XREADGROUP ... ">"` — read new messages
  - Budget split: each phase gets at most `maxCount / 2` to prevent starvation
- **XACK** in `MessagesDeliveredAsync()` — acknowledges delivered messages
- **XCLAIM recovery** in `Initialize()` — claims orphaned messages from dead consumers (idle > 60s)
- **XGROUP DELCONSUMER** in `Shutdown()` — removes consumer from group
- Sequence tokens derived from Redis entry ID (`{timestamp}-{seq}` → `EventSequenceTokenV2`)

### RedisStreamQueueMapper

- Implements `IStreamQueueMapper`
- Maps `StreamId` → partition using FNV-1a hash (deterministic across processes)
- Redis key format: `{prefix}:{partitionIndex}`

### RedisBatchContainer

- Implements `IBatchContainer`
- Carries deserialized events + Redis entry ID (for XACK)
- Sequence token = parsed Redis entry ID (monotonically increasing, restart-safe)

### RedisQueueCache

- Implements `IQueueCache`
- Per-stream `Dictionary<StreamId, List<IBatchContainer>>` for O(1) cursor lookup
- Watermark-based purge: items consumed by all cursors are removed
- Backpressure: `IsUnderPressure()` returns true when cache reaches `CacheSize`

## Data Flow

### Produce

```
1. Grain calls stream.OnNextAsync(event)
2. Orleans calls RedisStreamAdapter.QueueMessageBatchAsync()
3. Adapter serializes each event: Serializer.SerializeToArray(event) → byte[]
4. Adapter wraps in RedisStreamPayload { StreamId, EventPayloads, RequestContext }
5. Adapter serializes payload: Serializer.SerializeToArray(payload) → byte[]
6. Adapter calls XADD {key} MAXLEN ~ {maxLen} * data {bytes}
7. Redis returns entry ID (e.g., "1679000000000-0")
```

### Consume

```
1. PullingAgent timer fires (default: every 100ms)
2. Agent calls RedisStreamReceiver.GetQueueMessagesAsync(maxCount)
3. Receiver calls XREADGROUP GROUP {group} {consumer} COUNT {n} STREAMS {key} "0"  (pending)
4. Receiver calls XREADGROUP GROUP {group} {consumer} COUNT {n} STREAMS {key} ">"  (new)
5. For each entry: deserialize payload → deserialize each event byte[] → List<object>
6. Wrap in RedisBatchContainer with RedisEntryId and parsed sequence token
7. Agent adds to RedisQueueCache
8. Agent delivers to subscriber grain via OnNextAsync()
9. Agent calls RedisStreamReceiver.MessagesDeliveredAsync(delivered)
10. Receiver calls XACK {key} {group} {entryId1} {entryId2} ...
```

### Crash Recovery

```
1. Silo A reads messages via XREADGROUP but crashes before XACK
2. Messages remain in "pending" state in the consumer group
3. Silo B starts, RedisStreamReceiver.Initialize() runs
4. ClaimOrphanedPendingMessagesAsync():
   a. XPENDING {key} {group} — check for pending messages
   b. XPENDING {key} {group} - + 100 — get details (per-message idle time)
   c. Filter: idle > 60 seconds
   d. XCLAIM {key} {group} {consumer-B} {minIdleMs} {entryId1} {entryId2} ...
5. Claimed messages are now owned by Silo B's consumer
6. Next GetQueueMessagesAsync() phase-1 reads them via XREADGROUP "0"
7. After delivery, XACK clears them from pending
```

## Serialization

Events are serialized using Orleans' binary serializer (`Orleans.Serialization.Serializer`), which embeds full type information in each `byte[]` envelope. This means:

- Any `[GenerateSerializer]` type is correctly reconstructed on any silo
- Type identity is preserved across process boundaries
- No need for a shared schema registry

The serialization is two-level:
1. **Event level:** each event → `byte[]` (preserves concrete type)
2. **Payload level:** `RedisStreamPayload { StreamId, List<byte[]>, RequestContext }` → `byte[]`

The outer payload is what gets stored as the `data` field in the Redis Stream entry.

## Metrics

The provider emits `System.Diagnostics.Metrics` counters under meter `"Orleans.Streaming.Redis"`:

| Counter | Description |
|---------|-------------|
| `orleans.streaming.redis.messages_enqueued` | Messages successfully written via XADD |
| `orleans.streaming.redis.messages_dequeued` | Messages read via XREADGROUP |
| `orleans.streaming.redis.messages_acked` | Messages acknowledged via XACK |
| `orleans.streaming.redis.messages_failed` | Messages that failed serialization or XADD |
| `orleans.streaming.redis.messages_claimed` | Orphaned messages recovered via XCLAIM |

### OpenTelemetry integration

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Orleans.Streaming.Redis"));
```

## Dead Letter

When `DeadLetterPrefix` is configured, messages that fail deserialization are:

1. Forwarded to a dead-letter Redis Stream: `XADD {DeadLetterPrefix}:{partition} * data {rawBytes}`
2. XACK'd from the main stream (so they don't block processing)
3. Logged at `Error` level
4. Counted in `messages_failed` metric

Operators can inspect dead-letter streams manually:

```bash
redis-cli XRANGE deadletter:stream:0 - + COUNT 10
```

# Troubleshooting

## Common Issues

### Messages not being delivered to subscriber grains

**Symptom:** `stream.OnNextAsync()` succeeds but the subscriber grain's `OnNextAsync` is never called.

**Checklist:**

1. **PubSubStore configured?** Orleans requires a grain storage provider named `"PubSubStore"` for stream subscriptions. Without it, subscriptions are lost on silo restart.
   ```csharp
   silo.AddMemoryGrainStorage("PubSubStore");    // dev
   silo.AddRedisGrainStorage("PubSubStore", ...) // production
   ```

2. **Stream namespace matches?** `[ImplicitStreamSubscription("my-ns")]` must match the namespace in `GetStream<T>("my-ns", key)`.

3. **Grain key matches stream key?** With implicit subscriptions, the grain's primary key must match the stream key. Orleans activates the grain with the stream key as its identity.

4. **`[GenerateSerializer]` on event type?** The event type must have `[GenerateSerializer]` and `[Id(n)]` on properties. Without it, deserialization fails silently (check dead-letter stream if configured).

5. **Provider name matches?** The name in `AddRedisStreams("X", ...)` must match `this.GetStreamProvider("X")`.

6. **Pulling agent timing?** Default poll interval is 100ms. First delivery may take up to 200ms after produce. In tests, add `await Task.Delay(2000)` after producing.

### `BUSYGROUP` error in logs

**Symptom:** Log shows `RedisServerException: BUSYGROUP Consumer Group name already exists`.

**This is expected.** Each silo tries to create the consumer group on startup. If the group already exists, the error is caught and ignored. No action needed.

### `ConnectionString must not be empty` on startup

**Symptom:** `ArgumentException: ConnectionString must not be empty`.

**Fix:** Set the connection string in your `AddRedisStreams` call:
```csharp
silo.AddRedisStreams("StreamProvider", options =>
{
    options.ConnectionString = "localhost:6379";
});
```

Or use an environment variable:
```csharp
options.ConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")!;
```

### Messages re-delivered after silo restart

**Symptom:** After silo restart, some messages are delivered again.

**This is expected behavior.** Redis Streams provides at-least-once delivery. Messages read via `XREADGROUP` but not `XACK`'d before the silo crashed are re-delivered (via XCLAIM) after 60 seconds.

**Solution:** Make your subscriber grains idempotent. Use the `StreamSequenceToken` to detect duplicates:
```csharp
public Task OnNextAsync(MyEvent item, StreamSequenceToken? token)
{
    // token is derived from Redis entry ID — globally unique
    if (AlreadyProcessed(token)) return Task.CompletedTask;
    // ... process
}
```

### High `messages_failed` counter

**Symptom:** `orleans.streaming.redis.messages_failed` metric is increasing.

**Causes:**
1. **Event type changed** — a type was renamed/removed after messages were produced. Old messages can't be deserialized.
2. **Assembly version mismatch** — producer and consumer silos are running different versions with incompatible serialization.
3. **Redis XADD failure** — Redis is under memory pressure or unreachable.

**Diagnosis:** If `DeadLetterPrefix` is configured, inspect the dead-letter stream:
```bash
redis-cli XRANGE dlq:stream:0 - + COUNT 5
```

### Consumer lag growing

**Symptom:** `messages_enqueued` grows faster than `messages_acked`.

**Causes:**
1. **Slow consumer** — subscriber grain processing takes too long.
2. **Too few partitions** — bottleneck on a single pulling agent.
3. **Redis latency** — slow `XREADGROUP` responses.

**Solutions:**
- Increase `QueueCount` (requires redeployment)
- Decrease poll interval: `opt.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(50)`
- Increase `MaxBatchSize` for larger poll batches
- Profile subscriber grain for slow operations

### External consumer sees binary data instead of JSON

**Symptom:** A non-Orleans consumer reads from the Redis Stream but sees opaque binary blobs in the `data` field.

**Fix:** Enable JSON payload mode on the producer:
```csharp
options.PayloadMode = RedisStreamPayloadMode.Json;
```

After enabling, new entries will have human-readable fields (`stream_namespace`, `stream_key`, `payload`). Existing binary entries remain in the stream until they are trimmed by `MaxStreamLength`.

### JSON deserialization failures after schema change

**Symptom:** `messages_failed` counter increases after deploying a new event type schema.

**Causes:**
1. **Property renamed/removed** — JSON mode does not embed type information. If a property was renamed, old JSON entries may fail to deserialize into the new type.
2. **Type changed** — e.g., `int` → `string`. JSON deserialization is strict by default.

**Solutions:**
- Use `[JsonPropertyName("oldName")]` to maintain backward compatibility
- Configure `JsonSerializerOptions` with lenient settings if needed
- Enable `DeadLetterPrefix` to capture failed entries for inspection

### `XCLAIM` messages in logs

**Symptom:** Log shows `RedisStreamReceiver: claimed N orphaned pending messages`.

**This means crash recovery worked correctly.** A previous silo crashed, and the current silo claimed its pending messages. The claimed messages will be delivered and XACK'd.

If this happens frequently, investigate why silos are crashing.

## Redis CLI Debugging

```bash
# List all stream keys
redis-cli KEYS "orleans:stream:*"

# Check stream length per partition
redis-cli XLEN orleans:stream:0

# Show consumer group info
redis-cli XINFO GROUPS orleans:stream:0

# Show consumers in the group
redis-cli XINFO CONSUMERS orleans:stream:0 orleans

# Check pending messages
redis-cli XPENDING orleans:stream:0 orleans

# Detailed pending info (message IDs, idle time, delivery count)
redis-cli XPENDING orleans:stream:0 orleans - + 10

# Read last 5 entries
redis-cli XREVRANGE orleans:stream:0 + - COUNT 5

# Read dead-letter entries
redis-cli XRANGE dlq:stream:0 - + COUNT 10

# Monitor Redis commands in real-time
redis-cli MONITOR
```

## Logging

The provider uses `Microsoft.Extensions.Logging` throughout:

| Level | What's logged |
|-------|--------------|
| `Information` | Receiver initialization, shutdown, XCLAIM recovery |
| `Warning` | XACK failures (non-fatal), XREADGROUP errors, deserialization failures (without dead-letter) |
| `Error` | Deserialization failures (with dead-letter), delivery failures |
| `Debug` | XACK counts, consumer group operations |

To enable verbose logging:

```csharp
builder.Logging.AddFilter("Orleans.Streaming.Redis", LogLevel.Debug);
```

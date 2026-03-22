# Production Deployment

## Redis Requirements

### Version

**Redis 5.0+** is required (Streams support). Redis 7.x is recommended for best performance and `XAUTOCLAIM` support.

### Memory

Redis Streams consume memory proportional to the number of entries. Each entry stores:
- Entry ID (timestamp-sequence): ~16 bytes
- Field name (`data`): ~4 bytes
- Serialized payload: varies (typically 200 bytes – 10 KB per event batch)

With `MaxStreamLength = 10000` and 8 partitions, worst-case memory usage is approximately:

```
8 partitions × 10,000 entries × ~1 KB average = ~80 MB
```

Set `MaxStreamLength` based on your throughput and acceptable replay window.

### Persistence

Enable Redis persistence (RDB or AOF) to survive Redis restarts:

```
# redis.conf
appendonly yes
appendfsync everysec
```

Without persistence, all stream data is lost on Redis restart. Orleans PubSubStore (subscription state) should also be backed by persistent Redis.

### High Availability

For production, use Redis Sentinel or Redis Cluster:

```
# Sentinel connection string
sentinel1:26379,sentinel2:26379,sentinel3:26379,serviceName=mymaster,password=secret

# Cluster connection string (StackExchange.Redis auto-discovers topology)
node1:6379,node2:6379,node3:6379,password=secret
```

## Silo Configuration

### Recommended production settings

```csharp
silo.AddRedisStreams("StreamProvider",
    options =>
    {
        options.ConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")
            ?? throw new InvalidOperationException("REDIS_CONNECTION_STRING required");
        options.QueueCount = 16;              // scale with number of silos
        options.MaxStreamLength = 50_000;     // retention window
        options.MaxBatchSize = 100;           // messages per poll
        options.CacheSize = 8192;             // in-memory cache per partition
        options.DeadLetterPrefix = "dlq";     // enable dead-letter for debugging
    },
    configurePulling: configurator =>
    {
        configurator.Configure<StreamPullingAgentOptions>(ob =>
            ob.Configure(opt =>
            {
                opt.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                opt.MaxEventDeliveryTime = TimeSpan.FromMinutes(2);
            }));
    });

// PubSubStore MUST be persistent in production.
silo.AddRedisGrainStorage("PubSubStore", options =>
{
    options.ConnectionString = redisConnectionString;
});
```

### QueueCount sizing

| Silos | Recommended QueueCount |
|-------|----------------------|
| 1–2   | 4–8                  |
| 3–5   | 8–16                 |
| 6–10  | 16–32                |
| 10+   | 32–64                |

Orleans distributes queue partitions across silos. More partitions = better load distribution, but each partition has a pulling agent with its own poll timer.

**Important:** changing `QueueCount` after deployment creates new stream keys. Existing messages in old keys won't be consumed. Plan partition count before going to production.

### MaxStreamLength sizing

| Scenario | Recommended MaxStreamLength |
|----------|---------------------------|
| Low throughput (< 100 msg/s) | 10,000 |
| Medium throughput (100–1000 msg/s) | 50,000 |
| High throughput (1000+ msg/s) | 100,000+ |
| Guaranteed replay window | throughput × replay_seconds |

## Monitoring

### Metrics

Expose metrics via OpenTelemetry:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddMeter("Orleans.Streaming.Redis");
        metrics.AddPrometheusExporter();
    });
```

Key alerts to configure:

| Metric | Alert condition | Meaning |
|--------|----------------|---------|
| `messages_failed` | `rate > 0` | Serialization failures or Redis errors |
| `messages_enqueued - messages_acked` | `growing` | Consumer lag — messages not being processed |
| `messages_claimed` | `any > 0` | Silo crash recovery happened |

### Redis monitoring

```bash
# Check stream lengths
redis-cli XLEN orleans:stream:0
redis-cli XLEN orleans:stream:1
# ... for each partition

# Check consumer group state
redis-cli XINFO GROUPS orleans:stream:0

# Check pending messages (unacked)
redis-cli XPENDING orleans:stream:0 orleans

# Check dead-letter queue
redis-cli XLEN dlq:stream:0
redis-cli XRANGE dlq:stream:0 - + COUNT 5
```

### Health check

The provider does not expose a built-in health check. Monitor Redis connectivity at the infrastructure level:

```csharp
builder.Services.AddHealthChecks()
    .AddRedis(redisConnectionString, name: "redis-streams");
```

## Failure Modes

### Redis unavailable (transient)

- **Produce:** `QueueMessageBatchAsync` retries 3 times (100ms/200ms/400ms backoff), then throws `RedisException`. The grain call fails; Orleans retry policy applies.
- **Consume:** `GetQueueMessagesAsync` catches `RedisException`, returns empty list. Pulling agent retries on next poll cycle. No data loss — messages remain in Redis.

### Redis unavailable (prolonged)

- Produce side: all stream.OnNextAsync calls fail until Redis is back.
- Consume side: pulling agents poll but get empty responses. No data loss.
- After recovery: consumers catch up from where they left off (consumer group tracks offsets).

### Silo crash

- Pending messages (read but not XACK'd) are recovered by surviving silos via XCLAIM after 60 seconds idle time.
- PubSubStore subscriptions survive if backed by persistent storage (Redis/DB).

### Redis OOM

- `XADD` fails with OOM error. `MaxStreamLength` with approximate trimming should prevent this.
- Monitor `used_memory` and `maxmemory` in Redis.

### Message too large

- Orleans serializer has no built-in size limit. Very large events will work but increase Redis memory pressure.
- Consider using references (store large data externally, pass only IDs through streams).

## Scaling

### Horizontal scaling (add silos)

1. Deploy new silo with same `AddRedisStreams` configuration
2. Orleans automatically rebalances queue partitions across silos
3. New silo joins the consumer group and starts pulling from assigned partitions

No Redis changes needed. The consumer group handles offset tracking.

### Vertical scaling (more throughput)

1. Increase `QueueCount` (requires redeployment — new stream keys)
2. Decrease `GetQueueMsgsTimerPeriod` (more frequent polls)
3. Increase `MaxBatchSize` (more messages per poll)
4. Use Redis Cluster for read/write distribution

## Docker Compose example

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  my-orleans-app:
    build: .
    depends_on:
      redis:
        condition: service_healthy
    environment:
      - REDIS_CONNECTION_STRING=redis:6379

volumes:
  redis-data:
```

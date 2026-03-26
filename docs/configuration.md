# Configuration Reference

## RedisStreamOptions

All options are set via the `Action<RedisStreamOptions>` callback in `AddRedisStreams`:

```csharp
silo.AddRedisStreams("MyProvider", options =>
{
    options.ConnectionString = "redis-server:6379,password=secret";
    options.QueueCount = 16;
    options.MaxStreamLength = 50_000;
    // ... etc
});
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | StackExchange.Redis connection string. **Required.** Supports all [StackExchange.Redis configuration options](https://stackexchange.github.io/StackExchange.Redis/Configuration.html): password, SSL, timeouts, etc. |
| `QueueCount` | `int` | `8` | Number of stream partitions. Each partition is a separate Redis Stream key. More partitions = better parallelism across silos. Must be > 0. |
| `KeyPrefix` | `string` | `"orleans:stream"` | Redis key prefix for stream keys. Stream keys are formatted as `{KeyPrefix}:{partitionIndex}`. Must not be empty. |
| `ConsumerGroup` | `string` | `"orleans"` | Redis consumer group name. All silos in the cluster join this group. Redis tracks per-consumer offsets automatically. |
| `MaxBatchSize` | `int` | `100` | Maximum number of messages to read per `XREADGROUP` poll cycle. Higher values improve throughput but increase per-poll latency. Must be > 0. |
| `MaxStreamLength` | `int` | `10000` | `MAXLEN` argument for `XADD`. Caps the number of entries in each Redis Stream. Uses approximate trimming (`~`) for performance. Set to `0` for unlimited (not recommended in production). |
| `Database` | `int` | `-1` | Redis database index. `-1` uses the default database from the connection string. |
| `CacheSize` | `int` | `4096` | In-memory cache capacity (number of batch containers) per queue partition. When the cache fills up, backpressure signals the pulling agent to slow down. Must be > 0. |
| `PayloadMode` | `RedisStreamPayloadMode` | `Binary` | Controls event serialization format. `Binary` uses Orleans binary serializer (compact, opaque). `Json` uses `System.Text.Json` for human-readable entries that non-Orleans consumers can read. The read path auto-detects the format, so mixed entries work during rolling deployments. |
| `JsonSerializerOptions` | `JsonSerializerOptions?` | `null` | Custom `System.Text.Json.JsonSerializerOptions` for JSON payload mode. When `null`, defaults to camelCase naming, no indentation, and ignore-null-when-writing. Only used when `PayloadMode = Json`. |
| `DeadLetterPrefix` | `string?` | `null` | Redis key prefix for the dead-letter stream. When set, messages that fail deserialization are forwarded to `{DeadLetterPrefix}:{partitionIndex}` and XACK'd from the main stream. When `null`, failed messages are XACK'd and discarded (with error logging). |

### Validation

`RedisStreamOptions.Validate()` is called automatically when the adapter is created. It throws `ArgumentException` if:

- `ConnectionString` is null or whitespace
- `QueueCount` <= 0
- `MaxBatchSize` <= 0
- `KeyPrefix` is null or whitespace
- `CacheSize` <= 0

## Pulling Agent Configuration

The optional `configurePulling` callback exposes Orleans' built-in stream pulling agent settings:

```csharp
silo.AddRedisStreams("MyProvider",
    options => { options.ConnectionString = "localhost:6379"; },
    configurePulling: configurator =>
    {
        // How often the pulling agent polls Redis for new messages.
        // Lower = lower latency, higher = less Redis load.
        // Default: 100ms.
        configurator.Configure<StreamPullingAgentOptions>(ob =>
            ob.Configure(opt => opt.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(50)));
    });
```

Common `StreamPullingAgentOptions`:

| Option | Default | Description |
|--------|---------|-------------|
| `GetQueueMsgsTimerPeriod` | `100ms` | Poll interval for `XREADGROUP` |
| `InitQueueTimeout` | `5s` | Timeout for `Initialize` (consumer group creation) |
| `MaxEventDeliveryTime` | `1min` | Max time to deliver an event before it's considered failed |
| `StreamInactivityPeriod` | `30min` | How long a stream can be inactive before cleanup |

## Client Configuration

External Orleans clients (not silos) can produce to streams:

```csharp
var clientBuilder = new ClientBuilder();
clientBuilder.AddRedisStreams("MyProvider", options =>
{
    options.ConnectionString = "localhost:6379";
});
```

Clients can produce (`stream.OnNextAsync`) but do not consume — only silo grains with `[ImplicitStreamSubscription]` receive events.

## Connection String Examples

```
# Local Redis
localhost:6379

# With password
redis-server:6379,password=mypassword

# With SSL
redis-server:6380,ssl=true,password=mypassword

# Redis Sentinel
sentinel1:26379,sentinel2:26379,serviceName=mymaster

# Multiple endpoints (Redis Cluster)
node1:6379,node2:6379,node3:6379

# With all common options
redis:6379,password=secret,ssl=true,abortConnect=false,connectTimeout=5000,syncTimeout=3000
```

## Multiple Providers

You can register multiple Redis stream providers with different names and configurations:

```csharp
silo.AddRedisStreams("HighPriority", options =>
{
    options.ConnectionString = "redis-fast:6379";
    options.QueueCount = 16;
    options.MaxBatchSize = 50;
});

silo.AddRedisStreams("BulkProcessing", options =>
{
    options.ConnectionString = "redis-bulk:6379";
    options.QueueCount = 4;
    options.MaxBatchSize = 500;
    options.MaxStreamLength = 100_000;
});
```

Each provider manages its own Redis connection, consumer groups, and stream keys independently.

## JSON Payload Mode

Enable JSON mode to write human-readable entries for interop with non-Orleans consumers:

```csharp
silo.AddRedisStreams("InteropProvider", options =>
{
    options.ConnectionString = "localhost:6379";
    options.PayloadMode = RedisStreamPayloadMode.Json;
});
```

### Redis entry structure (JSON mode)

| Field | Content |
|-------|---------|
| `_payload_mode` | `"json"` — discriminator for auto-detection |
| `stream_namespace` | Orleans stream namespace |
| `stream_key` | Orleans stream key |
| `payload` | JSON array of serialized events |
| `request_context` | JSON object (only present if non-null) |

### Custom serialization options

```csharp
options.PayloadMode = RedisStreamPayloadMode.Json;
options.JsonSerializerOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = null,  // PascalCase instead of camelCase
    WriteIndented = true,         // pretty-print (not recommended for production)
};
```

### Auto-detection on read

The receiver automatically detects whether each entry is Binary or JSON by checking for the `_payload_mode` field. This means:

- **Rolling deployments** work seamlessly — old silos write Binary, new silos write JSON, and all silos can read both formats.
- The `PayloadMode` option only controls the **write** path.

### Non-Orleans consumer example (Node.js)

```javascript
const Redis = require('ioredis');
const redis = new Redis();

// Create consumer group (once)
await redis.xgroup('CREATE', 'orleans:stream:0', 'my-service', '0', 'MKSTREAM')
  .catch(err => { if (!err.message.includes('BUSYGROUP')) throw err; });

// Read messages
while (true) {
  const entries = await redis.xreadgroup(
    'GROUP', 'my-service', 'worker-1',
    'COUNT', 10, 'BLOCK', 5000,
    'STREAMS', 'orleans:stream:0', '>'
  );

  if (!entries) continue;

  for (const [, messages] of entries) {
    for (const [id, fields] of messages) {
      const obj = {};
      for (let i = 0; i < fields.length; i += 2)
        obj[fields[i]] = fields[i + 1];

      if (obj._payload_mode === 'json') {
        const events = JSON.parse(obj.payload);
        console.log(`Stream: ${obj.stream_namespace}/${obj.stream_key}`, events);
      }

      await redis.xack('orleans:stream:0', 'my-service', id);
    }
  }
}
```

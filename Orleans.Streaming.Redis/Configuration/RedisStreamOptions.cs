using System.Text.Json;

namespace Orleans.Streaming.Redis.Configuration;

/// <summary>
/// Configuration options for the Redis Streams persistent stream provider.
/// </summary>
public class RedisStreamOptions
{
    /// <summary>
    /// Redis connection string (StackExchange.Redis format).
    /// Required.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Number of queue partitions. Streams are distributed across partitions
    /// using consistent hashing. More partitions = better parallelism.
    /// Default: 8.
    /// </summary>
    public int QueueCount { get; set; } = 8;

    /// <summary>
    /// Redis key prefix for stream keys.
    /// Stream key format: {Prefix}:{queueIndex}
    /// Default: "orleans:stream".
    /// </summary>
    public string KeyPrefix { get; set; } = "orleans:stream";

    /// <summary>
    /// Consumer group name for XREADGROUP.
    /// Each silo joins this group; Redis tracks per-consumer offsets.
    /// Default: "orleans".
    /// </summary>
    public string ConsumerGroup { get; set; } = "orleans";

    /// <summary>
    /// Maximum number of messages to read per poll cycle.
    /// Default: 100.
    /// </summary>
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum length of each Redis Stream (MAXLEN argument to XADD).
    /// Prevents unbounded growth. 0 = no limit.
    /// Default: 10000.
    /// </summary>
    public int MaxStreamLength { get; set; } = 10_000;

    /// <summary>
    /// Redis database index. Default: -1 (use default database from connection string).
    /// </summary>
    public int Database { get; set; } = -1;

    /// <summary>
    /// In-memory cache size (number of batch containers) per queue partition.
    /// Default: 4096.
    /// </summary>
    public int CacheSize { get; set; } = 4096;

    /// <summary>
    /// Controls how events are serialized when written to Redis Streams.
    /// <see cref="RedisStreamPayloadMode.Binary"/> (default) uses Orleans binary serialization.
    /// <see cref="RedisStreamPayloadMode.Json"/> writes human-readable JSON, enabling
    /// interop with non-Orleans consumers.
    /// The read path auto-detects the format, so mixed Binary/Json entries are handled
    /// transparently during rolling deployments.
    /// </summary>
    public RedisStreamPayloadMode PayloadMode { get; set; } = RedisStreamPayloadMode.Binary;

    /// <summary>
    /// Custom <see cref="JsonSerializerOptions"/> used when <see cref="PayloadMode"/> is
    /// <see cref="RedisStreamPayloadMode.Json"/>. When <see langword="null"/>, sensible
    /// defaults are used (camelCase, no indentation, ignore null values).
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// Key prefix for the dead-letter stream. When set, messages that fail deserialization
    /// are forwarded to a Redis Stream at <c>{DeadLetterPrefix}:{queueIndex}</c> instead
    /// of being silently discarded. The failed entry is XACK'd from the main stream so
    /// it does not block future processing.
    /// Default: <see langword="null"/> (dead-letter disabled).
    /// </summary>
    public string? DeadLetterPrefix { get; set; } = null;

    /// <summary>
    /// Validates that all required options are set and have valid values.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any option is invalid.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString must not be empty.", nameof(ConnectionString));

        if (QueueCount <= 0)
            throw new ArgumentException($"QueueCount must be greater than 0, was {QueueCount}.", nameof(QueueCount));

        if (MaxBatchSize <= 0)
            throw new ArgumentException($"MaxBatchSize must be greater than 0, was {MaxBatchSize}.", nameof(MaxBatchSize));

        if (string.IsNullOrWhiteSpace(KeyPrefix))
            throw new ArgumentException("KeyPrefix must not be empty.", nameof(KeyPrefix));

        if (CacheSize <= 0)
            throw new ArgumentException($"CacheSize must be greater than 0, was {CacheSize}.", nameof(CacheSize));
    }
}

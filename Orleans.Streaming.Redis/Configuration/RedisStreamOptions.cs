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
}

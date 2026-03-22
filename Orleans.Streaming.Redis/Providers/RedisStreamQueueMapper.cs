using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Maps streams to a fixed set of Redis Stream queues using consistent hashing.
/// Queue ID = hash(streamId) mod QueueCount.
/// </summary>
public class RedisStreamQueueMapper : IStreamQueueMapper
{
    private readonly QueueId[] _queues;
    private readonly string _providerName;

    /// <summary>
    /// Initialises the mapper with the specified number of queue partitions.
    /// </summary>
    /// <param name="queueCount">Number of queue partitions. Must be greater than 0.</param>
    /// <param name="providerName">Stream provider name used to construct queue IDs.</param>
    public RedisStreamQueueMapper(int queueCount, string providerName)
    {
        _providerName = providerName;
        _queues = new QueueId[queueCount];
        for (var i = 0; i < queueCount; i++)
            _queues[i] = QueueId.GetQueueId(providerName, (uint)i, 0);
    }

    /// <inheritdoc />
    public IEnumerable<QueueId> GetAllQueues() => _queues;

    /// <inheritdoc />
    public QueueId GetQueueForStream(StreamId streamId)
    {
        var hash = StableHash(streamId.ToString());
        return _queues[hash % _queues.Length];
    }

    /// <summary>
    /// FNV-1a 32-bit hash — deterministic across processes and .NET versions.
    /// Unlike <c>string.GetHashCode()</c>, this is not randomised per-process,
    /// ensuring that all silos map the same <see cref="StreamId"/> to the same partition.
    /// </summary>
    private static uint StableHash(string input)
    {
        uint hash = 2166136261;
        foreach (var c in input)
        {
            hash ^= c;
            hash *= 16777619;
        }
        return hash;
    }

    /// <summary>
    /// Returns the Redis Stream key for a given queue ID.
    /// Format: {prefix}:{queueIndex}
    /// </summary>
    public static string GetRedisKey(string prefix, QueueId queueId)
        => $"{prefix}:{queueId.GetNumericId()}";
}

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

    public RedisStreamQueueMapper(int queueCount, string providerName)
    {
        _providerName = providerName;
        _queues = new QueueId[queueCount];
        for (var i = 0; i < queueCount; i++)
            _queues[i] = QueueId.GetQueueId(providerName, (uint)i, 0);
    }

    public IEnumerable<QueueId> GetAllQueues() => _queues;

    public QueueId GetQueueForStream(StreamId streamId)
    {
        var hash = (uint)streamId.GetHashCode();
        return _queues[hash % _queues.Length];
    }

    /// <summary>
    /// Returns the Redis Stream key for a given queue ID.
    /// Format: {prefix}:{queueIndex}
    /// </summary>
    public static string GetRedisKey(string prefix, QueueId queueId)
        => $"{prefix}:{queueId.GetNumericId()}";
}

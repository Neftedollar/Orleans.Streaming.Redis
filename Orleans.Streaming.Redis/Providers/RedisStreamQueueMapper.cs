using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Maps streams to a fixed set of Redis Stream queues using consistent hashing.
/// Queue ID = hash(streamId) mod QueueCount.
///
/// Implements <see cref="IConsistentRingStreamQueueMapper"/> so that Orleans can use
/// the default <c>ConsistentRingQueueBalancer</c> without requiring a separate balancer
/// configuration.  Queues are placed at evenly-spaced positions on the hash ring:
/// position[i] = (uint.MaxValue / QueueCount) * i.
/// </summary>
public class RedisStreamQueueMapper : IConsistentRingStreamQueueMapper
{
    private readonly QueueId[] _queues;
    // Pre-computed hash ring positions for each queue (evenly spaced).
    private readonly uint[] _ringPositions;
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
        _ringPositions = new uint[queueCount];

        var step = queueCount > 1 ? (uint)(uint.MaxValue / queueCount) : uint.MaxValue;
        for (var i = 0; i < queueCount; i++)
        {
            _queues[i] = QueueId.GetQueueId(providerName, (uint)i, 0);
            _ringPositions[i] = step * (uint)i;
        }
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
    /// Returns the queues whose hash ring positions fall within the given
    /// <paramref name="range"/>. Called by <c>ConsistentRingQueueBalancer</c> to
    /// determine which queues a silo is responsible for.
    /// </summary>
    /// <param name="range">The ring range assigned to a silo.</param>
    public IEnumerable<QueueId> GetQueuesForRange(IRingRange range)
    {
        for (var i = 0; i < _queues.Length; i++)
        {
            if (range.InRange(_ringPositions[i]))
                yield return _queues[i];
        }
    }

    /// <summary>
    /// FNV-1a 32-bit hash — deterministic across processes and .NET versions.
    /// Unlike <c>string.GetHashCode()</c>, this is not randomised per-process,
    /// ensuring that all silos map the same <see cref="StreamId"/> to the same partition.
    /// </summary>
    internal static uint StableHash(string input)
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

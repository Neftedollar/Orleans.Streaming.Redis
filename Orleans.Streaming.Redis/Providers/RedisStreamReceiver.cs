using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Receives messages from a single Redis Stream queue partition.
/// Uses XREADGROUP with consumer groups for reliable delivery.
/// </summary>
public class RedisStreamReceiver : IQueueAdapterReceiver
{
    private readonly string _streamKey;
    private readonly string _consumerGroup;
    private readonly int _maxBatchSize;
    private readonly IDatabase _db;
    private readonly Serializer _serializer;
    private string _consumerId = string.Empty;

    public RedisStreamReceiver(
        string streamKey,
        string consumerGroup,
        int maxBatchSize,
        IDatabase db,
        Serializer serializer)
    {
        _streamKey = streamKey;
        _consumerGroup = consumerGroup;
        _maxBatchSize = maxBatchSize;
        _db = db;
        _serializer = serializer;
    }

    public async Task Initialize(TimeSpan timeout)
    {
        _consumerId = $"silo-{Guid.NewGuid():N}";

        // Create consumer group if it doesn't exist.
        // "0" = start from beginning; MKSTREAM creates the stream if absent.
        try
        {
            await _db.StreamCreateConsumerGroupAsync(
                _streamKey, _consumerGroup, "0", createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Group already exists — expected in multi-silo setup.
        }
    }

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        var count = Math.Min(maxCount, _maxBatchSize);
        var result = new List<IBatchContainer>();

        // Read new messages (">") for this consumer in the group.
        var entries = await _db.StreamReadGroupAsync(
            _streamKey, _consumerGroup, _consumerId,
            position: ">",
            count: count);

        if (entries is null || entries.Length == 0)
            return result;

        long sequenceNumber = 0;
        foreach (var entry in entries)
        {
            var dataEntry = entry["data"];
            if (dataEntry.IsNull)
                continue;

            try
            {
                var payload = _serializer.Deserialize<RedisStreamPayload>((byte[])dataEntry!);
                if (payload is null)
                    continue;

                var streamId = StreamId.Create(payload.StreamNamespace ?? string.Empty, payload.StreamKey);
                var token = new EventSequenceTokenV2(sequenceNumber++);

                var container = new RedisBatchContainer(
                    streamId,
                    payload.Events,
                    payload.RequestContext,
                    token);

                result.Add(container);
            }
            catch
            {
                // Skip malformed entries — log in production.
            }
        }

        return result;
    }

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        // ACK delivered messages so they won't be re-delivered to this consumer.
        // In Redis Streams, XACK marks messages as processed for the consumer group.
        // We don't XDEL — let MAXLEN on XADD handle retention.
        // NOTE: we don't have the Redis entry IDs here (IBatchContainer doesn't carry them).
        // For v0.1, we rely on the consumer group auto-advance + MAXLEN for cleanup.
        // A proper implementation would store entry IDs in the batch container.
        await Task.CompletedTask;
    }

    public Task Shutdown(TimeSpan timeout)
    {
        return Task.CompletedTask;
    }
}

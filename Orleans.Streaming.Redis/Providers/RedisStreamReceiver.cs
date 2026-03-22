using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Receives messages from a single Redis Stream queue partition.
/// Uses XREADGROUP with consumer groups for reliable delivery.
/// Acknowledges delivered messages via XACK.
/// </summary>
public class RedisStreamReceiver : IQueueAdapterReceiver
{
    private readonly string _streamKey;
    private readonly string _consumerGroup;
    private readonly int _maxBatchSize;
    private readonly IDatabase _db;
    private readonly Serializer _serializer;
    private readonly ILogger? _logger;
    private string _consumerId = string.Empty;

    public RedisStreamReceiver(
        string streamKey,
        string consumerGroup,
        int maxBatchSize,
        IDatabase db,
        Serializer serializer,
        ILogger? logger = null)
    {
        _streamKey = streamKey;
        _consumerGroup = consumerGroup;
        _maxBatchSize = maxBatchSize;
        _db = db;
        _serializer = serializer;
        _logger = logger;
    }

    /// <inheritdoc />
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

        _logger?.LogInformation(
            "RedisStreamReceiver initialized: stream={StreamKey}, group={Group}, consumer={Consumer}",
            _streamKey, _consumerGroup, _consumerId);
    }

    /// <inheritdoc />
    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        var count = Math.Min(maxCount, _maxBatchSize);
        var result = new List<IBatchContainer>();

        // Read new messages (">") for this consumer in the group.
        StreamEntry[] entries;
        try
        {
            entries = await _db.StreamReadGroupAsync(
                _streamKey, _consumerGroup, _consumerId,
                position: ">",
                count: count);
        }
        catch (RedisException ex)
        {
            _logger?.LogWarning(ex, "RedisStreamReceiver: failed to read from {StreamKey}", _streamKey);
            return result;
        }

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
                    token)
                {
                    // Carry the Redis entry ID so MessagesDeliveredAsync can XACK it.
                    RedisEntryId = entry.Id.ToString()
                };

                result.Add(container);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex,
                    "RedisStreamReceiver: failed to deserialize entry {EntryId} from {StreamKey}",
                    entry.Id, _streamKey);
            }
        }

        return result;
    }

    /// <inheritdoc />
    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        if (messages is null || messages.Count == 0)
            return;

        // Collect Redis entry IDs from delivered batch containers.
        var entryIds = new List<RedisValue>();
        foreach (var msg in messages)
        {
            if (msg is RedisBatchContainer rbc && rbc.RedisEntryId is not null)
                entryIds.Add(rbc.RedisEntryId);
        }

        if (entryIds.Count == 0)
            return;

        try
        {
            // XACK: acknowledge messages as processed for this consumer group.
            // After XACK, messages won't appear in XPENDING and won't be re-delivered.
            var acked = await _db.StreamAcknowledgeAsync(
                _streamKey, _consumerGroup, [.. entryIds]);

            _logger?.LogDebug(
                "RedisStreamReceiver: XACK {Count}/{Total} entries on {StreamKey}",
                acked, entryIds.Count, _streamKey);
        }
        catch (RedisException ex)
        {
            // Non-fatal: unacked messages will be re-delivered on next XREADGROUP
            // with "0" position (pending entries). This is the Redis Streams
            // at-least-once delivery guarantee.
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: XACK failed for {Count} entries on {StreamKey}",
                entryIds.Count, _streamKey);
        }
    }

    /// <inheritdoc />
    public Task Shutdown(TimeSpan timeout)
    {
        _logger?.LogInformation(
            "RedisStreamReceiver shutting down: stream={StreamKey}, consumer={Consumer}",
            _streamKey, _consumerId);
        return Task.CompletedTask;
    }
}

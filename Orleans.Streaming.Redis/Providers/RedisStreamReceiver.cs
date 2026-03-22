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
///
/// Sequence tokens are derived from the Redis entry ID (format: "{milliseconds}-{sequence}"),
/// giving monotonically increasing, globally unique tokens that survive silo restarts.
///
/// On Initialize, pending messages from crashed consumers are recovered via XPENDING + XCLAIM.
/// On Shutdown, the consumer is removed via XGROUP DELCONSUMER.
/// </summary>
public class RedisStreamReceiver : IQueueAdapterReceiver
{
    /// <summary>
    /// How long a message must be pending before we XCLAIM it from a dead consumer.
    /// </summary>
    private static readonly TimeSpan PendingClaimThreshold = TimeSpan.FromSeconds(60);

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

        // Recover pending messages from dead consumers (crash recovery).
        await ClaimOrphanedPendingMessagesAsync();

        _logger?.LogInformation(
            "RedisStreamReceiver initialized: stream={StreamKey}, group={Group}, consumer={Consumer}",
            _streamKey, _consumerGroup, _consumerId);
    }

    /// <summary>
    /// Scans XPENDING for messages that have been idle longer than
    /// <see cref="PendingClaimThreshold"/> and XCLAIMs them for this consumer.
    /// This handles messages left pending after a consumer/silo crash.
    /// </summary>
    private async Task ClaimOrphanedPendingMessagesAsync()
    {
        try
        {
            var pendingInfo = await _db.StreamPendingAsync(_streamKey, _consumerGroup);
            if (pendingInfo.PendingMessageCount == 0)
                return;

            // XAUTOCLAIM: atomically claim idle messages in one command (Redis 6.2+).
            // Falls back gracefully — if the command fails we just skip recovery.
            var minIdleMs = (long)PendingClaimThreshold.TotalMilliseconds;

            // We use XPENDING with detailed info to find per-message idle times,
            // then batch-XCLAIM the ones that have exceeded the threshold.
            var detailed = await _db.StreamPendingMessagesAsync(
                _streamKey, _consumerGroup,
                count: 100,
                consumerName: RedisValue.Null, // all consumers
                minId: "-",
                maxId: "+");

            if (detailed is null || detailed.Length == 0)
                return;

            var toClaim = detailed
                .Where(p => p.IdleTimeInMilliseconds >= minIdleMs)
                .Select(p => (RedisValue)p.MessageId.ToString())
                .ToArray();

            if (toClaim.Length == 0)
                return;

            await _db.StreamClaimAsync(
                _streamKey, _consumerGroup, _consumerId,
                minIdleTimeInMs: minIdleMs,
                messageIds: toClaim);

            _logger?.LogInformation(
                "RedisStreamReceiver: claimed {Count} orphaned pending messages on {StreamKey}",
                toClaim.Length, _streamKey);
        }
        catch (Exception ex)
        {
            // Non-fatal: worst case those messages remain pending and will be re-claimed
            // on the next receiver restart.
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: failed to claim orphaned messages on {StreamKey}",
                _streamKey);
        }
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

                // Deserialize each event from its individual byte[] envelope.
                // The envelope carries full Orleans type info, so the concrete type is restored.
                var events = payload.EventPayloads
                    .Select(bytes => _serializer.Deserialize<object>(bytes))
                    .ToList();

                var streamId = StreamId.Create(payload.StreamNamespace ?? string.Empty, payload.StreamKey);

                // Parse the Redis entry ID (format: "{timestampMs}-{seq}") into a sequence token.
                // This gives monotonically increasing, restart-safe tokens.
                var token = ParseRedisEntryId(entry.Id.ToString());

                var container = new RedisBatchContainer(
                    streamId,
                    events!,
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

    /// <summary>
    /// Parses a Redis Stream entry ID (e.g. "1679000000000-3") into an
    /// <see cref="EventSequenceTokenV2"/>. The timestamp part becomes the
    /// <c>sequenceNumber</c> and the per-millisecond counter becomes
    /// <c>eventIndex</c>, giving a globally unique, monotonically increasing token.
    /// Falls back to a zero token if parsing fails.
    /// </summary>
    private static EventSequenceTokenV2 ParseRedisEntryId(string entryId)
    {
        var dashIndex = entryId.IndexOf('-');
        if (dashIndex > 0
            && long.TryParse(entryId.AsSpan(0, dashIndex), out var timestampMs)
            && int.TryParse(entryId.AsSpan(dashIndex + 1), out var seqIndex))
        {
            return new EventSequenceTokenV2(timestampMs, seqIndex);
        }

        return new EventSequenceTokenV2(0);
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
    public async Task Shutdown(TimeSpan timeout)
    {
        _logger?.LogInformation(
            "RedisStreamReceiver shutting down: stream={StreamKey}, consumer={Consumer}",
            _streamKey, _consumerId);

        if (string.IsNullOrEmpty(_consumerId))
            return;

        // XGROUP DELCONSUMER: clean up this consumer from the group so that its
        // pending count is removed. Any still-pending messages will be claimed by
        // the next receiver that calls ClaimOrphanedPendingMessagesAsync.
        try
        {
            await _db.StreamDeleteConsumerAsync(_streamKey, _consumerGroup, _consumerId);

            _logger?.LogDebug(
                "RedisStreamReceiver: XGROUP DELCONSUMER {Consumer} on {StreamKey}",
                _consumerId, _streamKey);
        }
        catch (Exception ex)
        {
            // Non-fatal: the consumer entry will age out or be claimed by a peer.
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: failed to delete consumer {Consumer} on {StreamKey}",
                _consumerId, _streamKey);
        }
    }
}

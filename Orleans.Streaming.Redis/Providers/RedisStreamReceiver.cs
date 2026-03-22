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
    /// Fix #7: Scans XPENDING in pages of 100 (max 10 iterations) for messages that have
    /// been idle longer than <see cref="PendingClaimThreshold"/> and XCLAIMs them for this consumer.
    /// This handles messages left pending after a consumer/silo crash.
    /// </summary>
    private async Task ClaimOrphanedPendingMessagesAsync()
    {
        try
        {
            var pendingInfo = await _db.StreamPendingAsync(_streamKey, _consumerGroup);
            if (pendingInfo.PendingMessageCount == 0)
                return;

            var minIdleMs = (long)PendingClaimThreshold.TotalMilliseconds;
            const int pageSize = 100;
            const int maxIterations = 10;
            var totalClaimed = 0;

            for (var iteration = 0; iteration < maxIterations; iteration++)
            {
                var detailed = await _db.StreamPendingMessagesAsync(
                    _streamKey, _consumerGroup,
                    count: pageSize,
                    consumerName: RedisValue.Null, // all consumers
                    minId: "-",
                    maxId: "+");

                if (detailed is null || detailed.Length == 0)
                    break;

                var toClaim = detailed
                    .Where(p => p.IdleTimeInMilliseconds >= minIdleMs)
                    .Select(p => (RedisValue)p.MessageId.ToString())
                    .ToArray();

                if (toClaim.Length == 0)
                    break;

                await _db.StreamClaimAsync(
                    _streamKey, _consumerGroup, _consumerId,
                    minIdleTimeInMs: minIdleMs,
                    messageIds: toClaim);

                totalClaimed += toClaim.Length;

                // If we got fewer than a full page, there are no more pending entries.
                if (detailed.Length < pageSize)
                    break;
            }

            if (totalClaimed > 0)
            {
                RedisStreamMetrics.MessagesClaimed.Add(totalClaimed);
                _logger?.LogInformation(
                    "RedisStreamReceiver: claimed {Count} orphaned pending messages on {StreamKey}",
                    totalClaimed, _streamKey);
            }
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

    /// <summary>
    /// Fix #3: Two-phase read:
    /// 1. XREADGROUP "0" — read pending entries already assigned to this consumer
    ///    (e.g. messages claimed from a crashed peer via ClaimOrphanedPendingMessagesAsync).
    /// 2. XREADGROUP ">" — read new entries not yet delivered to any consumer.
    /// Both results are combined in a single batch.
    /// </summary>
    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        var count = Math.Min(maxCount, _maxBatchSize);
        var result = new List<IBatchContainer>();

        // Phase 1: read pending entries for this consumer (position "0").
        // These are messages that were claimed but not yet ACKed — e.g. claimed from a
        // dead peer. XREADGROUP ">" would skip them; "0" returns them.
        StreamEntry[]? pendingEntries = null;
        try
        {
            pendingEntries = await _db.StreamReadGroupAsync(
                _streamKey, _consumerGroup, _consumerId,
                position: "0",
                count: count);
        }
        catch (RedisException ex)
        {
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: failed to read pending entries from {StreamKey}", _streamKey);
        }

        // Phase 2: read new (undelivered) entries (position ">").
        StreamEntry[]? newEntries = null;
        try
        {
            // Reduce count by how many pending we got so we don't exceed maxCount total.
            var pendingCount = pendingEntries?.Length ?? 0;
            var remaining = Math.Max(1, count - pendingCount);

            newEntries = await _db.StreamReadGroupAsync(
                _streamKey, _consumerGroup, _consumerId,
                position: ">",
                count: remaining);
        }
        catch (RedisException ex)
        {
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: failed to read from {StreamKey}", _streamKey);
        }

        // Combine both phases.
        var allEntries = Combine(pendingEntries, newEntries);
        if (allEntries.Length == 0)
            return result;

        foreach (var entry in allEntries)
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
                var events = payload.EventPayloads
                    .Select(bytes => _serializer.Deserialize<object>(bytes))
                    .ToList();

                var streamId = StreamId.Create(payload.StreamNamespace ?? string.Empty, payload.StreamKey);

                var token = ParseRedisEntryId(entry.Id.ToString());

                var container = new RedisBatchContainer(
                    streamId,
                    events!,
                    payload.RequestContext,
                    token)
                {
                    RedisEntryId = entry.Id.ToString()
                };

                result.Add(container);
                RedisStreamMetrics.MessagesDequeued.Add(1);
            }
            catch (Exception ex)
            {
                RedisStreamMetrics.MessagesFailed.Add(1);
                _logger?.LogWarning(ex,
                    "RedisStreamReceiver: failed to deserialize entry {EntryId} from {StreamKey}",
                    entry.Id, _streamKey);
            }
        }

        return result;
    }

    private static StreamEntry[] Combine(StreamEntry[]? a, StreamEntry[]? b)
    {
        if (a is null || a.Length == 0) return b ?? [];
        if (b is null || b.Length == 0) return a;
        var combined = new StreamEntry[a.Length + b.Length];
        a.CopyTo(combined, 0);
        b.CopyTo(combined, a.Length);
        return combined;
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
            var acked = await _db.StreamAcknowledgeAsync(
                _streamKey, _consumerGroup, [.. entryIds]);

            RedisStreamMetrics.MessagesAcked.Add(acked);

            _logger?.LogDebug(
                "RedisStreamReceiver: XACK {Count}/{Total} entries on {StreamKey}",
                acked, entryIds.Count, _streamKey);
        }
        catch (RedisException ex)
        {
            // Non-fatal: unacked messages will be re-delivered via the "0" phase on next poll.
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

        try
        {
            await _db.StreamDeleteConsumerAsync(_streamKey, _consumerGroup, _consumerId);

            _logger?.LogDebug(
                "RedisStreamReceiver: XGROUP DELCONSUMER {Consumer} on {StreamKey}",
                _consumerId, _streamKey);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: failed to delete consumer {Consumer} on {StreamKey}",
                _consumerId, _streamKey);
        }
    }
}

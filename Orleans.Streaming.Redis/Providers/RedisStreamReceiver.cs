using System.Text.Json;
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
    private readonly string? _deadLetterStreamKey;
    private readonly JsonSerializerOptions _jsonOptions;
    private string _consumerId = string.Empty;

    /// <summary>
    /// Tracks entry IDs that have been read from Redis (via XREADGROUP ">") but not yet
    /// acknowledged. Used to prevent phase-1 ("0") re-reads from returning messages that are
    /// currently in-flight (being delivered to subscribers). Only messages that have been in
    /// the PEL since before this consumer's lifetime (crash recovery) should come from phase-1.
    /// </summary>
    private readonly HashSet<string> _inFlightEntryIds = [];

    /// <summary>
    /// Initialises a new <see cref="RedisStreamReceiver"/>.
    /// </summary>
    /// <param name="streamKey">Redis Stream key for this queue partition.</param>
    /// <param name="consumerGroup">Redis consumer group name.</param>
    /// <param name="maxBatchSize">Maximum number of messages per poll cycle.</param>
    /// <param name="db">Redis database handle.</param>
    /// <param name="serializer">Orleans serializer for payload deserialization.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="deadLetterStreamKey">
    /// Optional Redis Stream key for dead-letter entries. When non-null, messages
    /// that fail deserialization are forwarded here and XACK'd from the main stream.
    /// </param>
    /// <param name="jsonSerializerOptions">
    /// JSON serializer options used to deserialize JSON-mode entries. The read path
    /// auto-detects the payload format, so these options are always needed.
    /// </param>
    public RedisStreamReceiver(
        string streamKey,
        string consumerGroup,
        int maxBatchSize,
        IDatabase db,
        Serializer serializer,
        ILogger? logger = null,
        string? deadLetterStreamKey = null,
        JsonSerializerOptions? jsonSerializerOptions = null)
    {
        _streamKey = streamKey;
        _consumerGroup = consumerGroup;
        _maxBatchSize = maxBatchSize;
        _db = db;
        _serializer = serializer;
        _logger = logger;
        _deadLetterStreamKey = deadLetterStreamKey;
        _jsonOptions = jsonSerializerOptions ?? JsonPayloadSerializer.DefaultOptions;
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
    /// Two-phase read with split budget to prevent pending-message starvation:
    /// <list type="number">
    ///   <item><description>XREADGROUP "0" — read pending entries already assigned to this consumer
    ///   (e.g. messages claimed from a crashed peer via ClaimOrphanedPendingMessagesAsync).
    ///   Budget: min(count/2, pendingCount) so new messages are never starved.</description></item>
    ///   <item><description>XREADGROUP ">" — read new entries not yet delivered to any consumer.
    ///   Budget: remaining after phase 1.</description></item>
    /// </list>
    /// </summary>
    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        var count = Math.Min(maxCount, _maxBatchSize);
        var result = new List<IBatchContainer>();

        // Phase 1: read pending entries for this consumer (position "0").
        // These are entries that were assigned to this consumer but not yet ACK'd.
        // Entries that are currently in-flight (tracked by _inFlightEntryIds) are skipped
        // here to avoid re-delivering messages that are already being processed.
        var phase1Budget = Math.Max(1, count / 2);
        StreamEntry[]? pendingEntries = null;
        try
        {
            var rawPending = await _db.StreamReadGroupAsync(
                _streamKey, _consumerGroup, _consumerId,
                position: "0",
                count: phase1Budget);

            // Exclude entries already tracked as in-flight to prevent double-delivery
            // during the window between a XREADGROUP ">" read and its XACK.
            pendingEntries = rawPending is null || rawPending.Length == 0
                ? rawPending
                : rawPending.Where(e => !_inFlightEntryIds.Contains(e.Id.ToString())).ToArray();
        }
        catch (RedisException ex)
        {
            _logger?.LogWarning(ex,
                "RedisStreamReceiver: failed to read pending entries from {StreamKey}", _streamKey);
        }

        // Phase 2: read new (undelivered) entries (position ">").
        // Gets the remaining budget after phase 1.
        StreamEntry[]? newEntries = null;
        try
        {
            var pendingCount = pendingEntries?.Length ?? 0;
            var remaining = Math.Max(1, count - pendingCount);

            newEntries = await _db.StreamReadGroupAsync(
                _streamKey, _consumerGroup, _consumerId,
                position: ">",
                count: remaining);

            // Track new entries as in-flight until they are ACK'd.
            if (newEntries is not null)
            {
                foreach (var entry in newEntries)
                    _inFlightEntryIds.Add(entry.Id.ToString());
            }
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
            try
            {
                RedisBatchContainer container;
                var token = ParseRedisEntryId(entry.Id.ToString());

                if (JsonPayloadSerializer.IsJsonEntry(entry))
                {
                    // JSON payload mode — auto-detected via discriminator field.
                    var (streamId, jsonEvents, requestContext) =
                        JsonPayloadSerializer.Deserialize(entry, _jsonOptions);

                    container = new RedisBatchContainer(
                        streamId, jsonEvents, _jsonOptions, requestContext, token)
                    {
                        RedisEntryId = entry.Id.ToString()
                    };
                }
                else
                {
                    // Binary payload mode (default).
                    var dataEntry = entry["data"];
                    if (dataEntry.IsNull)
                        continue;

                    var payload = _serializer.Deserialize<RedisStreamPayload>((byte[])dataEntry!);
                    if (payload is null)
                        continue;

                    var events = payload.EventPayloads
                        .Select(bytes => _serializer.Deserialize<object>(bytes))
                        .ToList();

                    var streamId = StreamId.Create(payload.StreamNamespace ?? string.Empty, payload.StreamKey);

                    container = new RedisBatchContainer(
                        streamId, events!, payload.RequestContext, token)
                    {
                        RedisEntryId = entry.Id.ToString()
                    };
                }

                result.Add(container);
                RedisStreamMetrics.MessagesDequeued.Add(1);
            }
            catch (Exception ex)
            {
                RedisStreamMetrics.MessagesFailed.Add(1);
                _logger?.LogError(ex,
                    "RedisStreamReceiver: failed to deserialize entry {EntryId} from {StreamKey}. " +
                    "Entry will be XACK'd to prevent redelivery loop.",
                    entry.Id, _streamKey);

                // XACK the bad entry so it does not remain in the pending list and block
                // future processing. A deserialization failure is not recoverable by retry.
                try
                {
                    await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entry.Id);
                }
                catch (Exception ackEx)
                {
                    _logger?.LogWarning(ackEx,
                        "RedisStreamReceiver: XACK of failed entry {EntryId} on {StreamKey} failed",
                        entry.Id, _streamKey);
                }

                // Forward to dead-letter stream if configured.
                if (_deadLetterStreamKey is not null)
                {
                    try
                    {
                        var dlData = entry["data"];
                        var rawPayload = dlData.IsNull ? RedisValue.EmptyString : dlData;
                        await _db.StreamAddAsync(
                            _deadLetterStreamKey,
                            [
                                new NameValueEntry("source_stream", _streamKey),
                                new NameValueEntry("source_id", entry.Id.ToString()),
                                new NameValueEntry("error", ex.Message),
                                new NameValueEntry("data", rawPayload),
                            ]);
                    }
                    catch (Exception dlEx)
                    {
                        _logger?.LogWarning(dlEx,
                            "RedisStreamReceiver: failed to write dead-letter entry {EntryId} to {DeadLetterKey}",
                            entry.Id, _deadLetterStreamKey);
                    }
                }
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
    internal static EventSequenceTokenV2 ParseRedisEntryId(string entryId)
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

            // Remove ACK'd entries from the in-flight set.
            foreach (var id in entryIds)
                _inFlightEntryIds.Remove((string)id!);
        }
        catch (RedisException ex)
        {
            // Non-fatal: unacked messages will be re-delivered via the "0" phase on next poll.
            // Note: in-flight IDs remain in the set so they are still excluded from phase-1
            // re-reads until ACK succeeds on a subsequent attempt.
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

        _inFlightEntryIds.Clear();

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

using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.Redis.Configuration;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Orleans queue adapter backed by Redis Streams.
/// Producer side: XADD to the appropriate stream key (with retry on transient failures).
/// Consumer side: creates RedisStreamReceiver per queue.
/// </summary>
public class RedisStreamAdapter : IQueueAdapter
{
    private readonly RedisStreamOptions _options;
    private readonly IConnectionMultiplexer _redis;
    private readonly Serializer _serializer;
    private readonly RedisStreamQueueMapper _queueMapper;
    private readonly ILoggerFactory? _loggerFactory;

    /// <summary>
    /// Initialises the adapter.
    /// </summary>
    /// <param name="providerName">Stream provider name.</param>
    /// <param name="options">Redis stream configuration options.</param>
    /// <param name="redis">Active Redis connection multiplexer.</param>
    /// <param name="serializer">Orleans serializer for payload serialization/deserialization.</param>
    /// <param name="queueMapper">Maps stream IDs to queue partition IDs.</param>
    /// <param name="loggerFactory">Optional logger factory.</param>
    public RedisStreamAdapter(
        string providerName,
        RedisStreamOptions options,
        IConnectionMultiplexer redis,
        Serializer serializer,
        RedisStreamQueueMapper queueMapper,
        ILoggerFactory? loggerFactory = null)
    {
        Name = providerName;
        _options = options;
        _redis = redis;
        _serializer = serializer;
        _queueMapper = queueMapper;
        _loggerFactory = loggerFactory;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public bool IsRewindable => true;

    /// <inheritdoc />
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    /// <summary>
    /// Creates a receiver for the specified queue partition.
    /// If <see cref="RedisStreamOptions.DeadLetterPrefix"/> is configured, the receiver
    /// will forward undeserializable messages to the dead-letter stream.
    /// </summary>
    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var db = _redis.GetDatabase(_options.Database);
        var streamKey = RedisStreamQueueMapper.GetRedisKey(_options.KeyPrefix, queueId);

        string? deadLetterKey = null;
        if (_options.DeadLetterPrefix is not null)
            deadLetterKey = RedisStreamQueueMapper.GetRedisKey(_options.DeadLetterPrefix, queueId);

        return new RedisStreamReceiver(
            streamKey,
            _options.ConsumerGroup,
            _options.MaxBatchSize,
            db,
            _serializer,
            _loggerFactory?.CreateLogger<RedisStreamReceiver>(),
            deadLetterStreamKey: deadLetterKey,
            jsonSerializerOptions: _options.JsonSerializerOptions ?? JsonPayloadSerializer.DefaultOptions);
    }

    /// <summary>
    /// Serializes events and enqueues them via XADD with exponential backoff retry
    /// (3 attempts: 100ms, 200ms, 400ms) on transient Redis failures.
    /// </summary>
    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId,
        IEnumerable<T> events,
        StreamSequenceToken? token,
        Dictionary<string, object>? requestContext)
    {
        var queueId = _queueMapper.GetQueueForStream(streamId);
        var streamKey = RedisStreamQueueMapper.GetRedisKey(_options.KeyPrefix, queueId);
        var db = _redis.GetDatabase(_options.Database);

        var entries = _options.PayloadMode == Configuration.RedisStreamPayloadMode.Json
            ? JsonPayloadSerializer.Serialize(streamId, events, requestContext,
                _options.JsonSerializerOptions ?? JsonPayloadSerializer.DefaultOptions)
            : [new NameValueEntry("data", SerializeBatch(streamId, events, requestContext))];

        var maxLen = _options.MaxStreamLength > 0 ? _options.MaxStreamLength : (int?)null;

        // Fix #1: simple exponential backoff retry — 3 attempts, 100ms/200ms/400ms.
        const int maxAttempts = 3;
        var delayMs = 100;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await db.StreamAddAsync(
                    streamKey,
                    entries,
                    maxLength: maxLen,
                    useApproximateMaxLength: true);

                RedisStreamMetrics.MessagesEnqueued.Add(1);
                return;
            }
            catch (RedisException) when (attempt < maxAttempts)
            {
                await Task.Delay(delayMs);
                delayMs *= 2;
            }
            catch (Exception)
            {
                RedisStreamMetrics.MessagesFailed.Add(1);
                throw;
            }
        }

        // Should not reach here, but satisfy the compiler.
        RedisStreamMetrics.MessagesFailed.Add(1);
    }

    private byte[] SerializeBatch<T>(
        StreamId streamId,
        IEnumerable<T> events,
        Dictionary<string, object>? requestContext)
    {
        // Serialize each event individually with full type info so that
        // deserialization on a different silo can reconstruct the concrete type.
        var eventPayloads = events
            .Cast<object>()
            .Select(e => _serializer.SerializeToArray(e))
            .ToList();

        var container = new RedisStreamPayload(
            StreamIdString: streamId.ToString(),
            StreamNamespace: streamId.GetNamespace(),
            StreamKey: streamId.GetKeyAsString(),
            EventPayloads: eventPayloads,
            RequestContext: requestContext);

        return _serializer.SerializeToArray(container);
    }
}

/// <summary>
/// Serializable payload stored in each Redis Stream entry.
/// Each event is pre-serialized as a byte[] envelope that carries full Orleans type info,
/// enabling correct round-trip deserialization across silo boundaries.
/// </summary>
[GenerateSerializer]
internal record RedisStreamPayload(
    [property: Id(0)] string StreamIdString,
    [property: Id(1)] string? StreamNamespace,
    [property: Id(2)] string StreamKey,
    [property: Id(3)] List<byte[]> EventPayloads,
    [property: Id(4)] Dictionary<string, object>? RequestContext);

using System.Diagnostics.Metrics;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Metrics for the Redis Streams provider using System.Diagnostics.Metrics.
/// Consumers can listen via <see cref="MeterListener"/> or any OpenTelemetry exporter.
/// Meter name: "Orleans.Streaming.Redis".
/// </summary>
internal static class RedisStreamMetrics
{
    private static readonly Meter Meter = new("Orleans.Streaming.Redis");

    /// <summary>Number of messages successfully enqueued via XADD.</summary>
    internal static readonly Counter<long> MessagesEnqueued =
        Meter.CreateCounter<long>(
            "orleans.streaming.redis.messages_enqueued",
            unit: "{messages}",
            description: "Number of messages enqueued via XADD.");

    /// <summary>Number of messages dequeued via XREADGROUP.</summary>
    internal static readonly Counter<long> MessagesDequeued =
        Meter.CreateCounter<long>(
            "orleans.streaming.redis.messages_dequeued",
            unit: "{messages}",
            description: "Number of messages dequeued via XREADGROUP.");

    /// <summary>Number of messages acknowledged via XACK.</summary>
    internal static readonly Counter<long> MessagesAcked =
        Meter.CreateCounter<long>(
            "orleans.streaming.redis.messages_acked",
            unit: "{messages}",
            description: "Number of messages acknowledged via XACK.");

    /// <summary>Number of messages that failed during enqueue or deserialization.</summary>
    internal static readonly Counter<long> MessagesFailed =
        Meter.CreateCounter<long>(
            "orleans.streaming.redis.messages_failed",
            unit: "{messages}",
            description: "Number of messages that failed during enqueue or deserialization.");

    /// <summary>Number of orphaned pending messages claimed via XCLAIM.</summary>
    internal static readonly Counter<long> MessagesClaimed =
        Meter.CreateCounter<long>(
            "orleans.streaming.redis.messages_claimed",
            unit: "{messages}",
            description: "Number of orphaned pending messages claimed via XCLAIM.");
}

namespace Orleans.Streaming.Redis.Configuration;

/// <summary>
/// Controls how events are serialized when written to Redis Streams.
/// </summary>
public enum RedisStreamPayloadMode
{
    /// <summary>
    /// Orleans binary serialization (default). Compact and efficient, but opaque
    /// to non-Orleans consumers.
    /// </summary>
    Binary = 0,

    /// <summary>
    /// JSON via System.Text.Json. Human-readable entries in Redis, enabling
    /// interop with non-Orleans consumers (Node.js, Python, Go, etc.).
    /// </summary>
    Json = 1,
}

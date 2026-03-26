using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Handles JSON serialization/deserialization of stream events for the JSON payload mode.
/// Produces multi-field Redis Stream entries that are human-readable and consumable
/// by non-Orleans clients (Node.js, Python, Go, etc.).
/// </summary>
internal static class JsonPayloadSerializer
{
    /// <summary>
    /// Discriminator field written to JSON-mode entries so the receiver can auto-detect the format.
    /// </summary>
    internal const string PayloadModeField = "_payload_mode";
    internal const string PayloadModeValue = "json";

    internal const string StreamNamespaceField = "stream_namespace";
    internal const string StreamKeyField = "stream_key";
    internal const string PayloadField = "payload";
    internal const string RequestContextField = "request_context";

    /// <summary>
    /// Default <see cref="JsonSerializerOptions"/> used when the user does not provide custom options.
    /// </summary>
    internal static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Serializes a batch of events into Redis Stream <see cref="NameValueEntry"/> fields
    /// suitable for <c>XADD</c>.
    /// </summary>
    internal static NameValueEntry[] Serialize<T>(
        StreamId streamId,
        IEnumerable<T> events,
        Dictionary<string, object>? requestContext,
        JsonSerializerOptions options)
    {
        var eventsArray = events.Cast<object>().Select(e => JsonSerializer.SerializeToElement(e, e.GetType(), options)).ToArray();
        var payloadJson = JsonSerializer.Serialize(eventsArray, options);

        var entries = new List<NameValueEntry>(5)
        {
            new(PayloadModeField, PayloadModeValue),
            new(StreamNamespaceField, streamId.GetNamespace() ?? string.Empty),
            new(StreamKeyField, streamId.GetKeyAsString()),
            new(PayloadField, payloadJson),
        };

        if (requestContext is { Count: > 0 })
        {
            var ctxJson = JsonSerializer.Serialize(requestContext, options);
            entries.Add(new NameValueEntry(RequestContextField, ctxJson));
        }

        return entries.ToArray();
    }

    /// <summary>
    /// Deserializes a JSON-mode Redis Stream entry back into stream metadata and event elements.
    /// </summary>
    internal static (StreamId StreamId, List<JsonElement> Events, Dictionary<string, object>? RequestContext) Deserialize(
        StreamEntry entry,
        JsonSerializerOptions options)
    {
        var ns = (string?)entry[StreamNamespaceField] ?? string.Empty;
        var key = (string)entry[StreamKeyField]!;
        var payloadJson = (string)entry[PayloadField]!;

        var streamId = StreamId.Create(ns, key);
        var elements = JsonSerializer.Deserialize<List<JsonElement>>(payloadJson, options)
                       ?? [];

        Dictionary<string, object>? requestContext = null;
        var ctxValue = entry[RequestContextField];
        if (!ctxValue.IsNull)
        {
            var ctxJson = (string)ctxValue!;
            requestContext = JsonSerializer.Deserialize<Dictionary<string, object>>(ctxJson, options);
        }

        return (streamId, elements, requestContext);
    }

    /// <summary>
    /// Checks whether a Redis Stream entry was written in JSON mode by looking for the
    /// <see cref="PayloadModeField"/> discriminator.
    /// </summary>
    internal static bool IsJsonEntry(StreamEntry entry)
    {
        var mode = entry[PayloadModeField];
        return !mode.IsNull && (string)mode! == PayloadModeValue;
    }
}

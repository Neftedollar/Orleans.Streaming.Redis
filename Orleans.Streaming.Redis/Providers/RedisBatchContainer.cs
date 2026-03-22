using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Wraps events from a single Redis Stream entry into an Orleans IBatchContainer.
/// Each entry maps to one batch container with a sequence token derived from the Redis entry ID.
/// </summary>
[GenerateSerializer]
public class RedisBatchContainer : IBatchContainer
{
    [Id(0)] private readonly StreamId _streamId;
    [Id(1)] private readonly List<object> _events;
    [Id(2)] private readonly Dictionary<string, object>? _requestContext;
    [Id(3)] private readonly EventSequenceTokenV2 _sequenceToken;

    public RedisBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object>? requestContext,
        EventSequenceTokenV2 sequenceToken)
    {
        _streamId = streamId;
        _events = events;
        _requestContext = requestContext;
        _sequenceToken = sequenceToken;
    }

    public StreamId StreamId => _streamId;
    public StreamSequenceToken SequenceToken => _sequenceToken;

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        return _events
            .OfType<T>()
            .Select((e, i) => Tuple.Create(e, (StreamSequenceToken)_sequenceToken.CreateSequenceTokenForEvent(i)));
    }

    public bool ImportRequestContext()
    {
        if (_requestContext is null)
            return false;

        foreach (var kvp in _requestContext)
            RequestContext.Set(kvp.Key, kvp.Value);

        return true;
    }
}

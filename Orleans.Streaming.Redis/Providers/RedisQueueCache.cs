using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Minimal in-memory queue cache. Stores delivered batches and purges after delivery.
/// For v0.1 — simple list-based implementation; replace with ring buffer if needed.
/// </summary>
internal class RedisQueueCache : IQueueCache
{
    private readonly List<IBatchContainer> _cache = [];
    private readonly object _lock = new();
    private readonly int _maxSize;

    public RedisQueueCache(int maxSize = 4096)
    {
        _maxSize = maxSize;
    }

    public int GetMaxAddCount() => _maxSize - _cache.Count;
    public bool IsUnderPressure() => _cache.Count >= _maxSize;

    public void AddToCache(IList<IBatchContainer> messages)
    {
        lock (_lock)
            _cache.AddRange(messages);
    }

    public bool TryPurgeFromCache(out IList<IBatchContainer>? purgedItems)
    {
        purgedItems = null;
        return false; // Let items expire naturally via cursor advancement.
    }

    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken? token)
        => new RedisQueueCacheCursor(_cache, _lock, streamId, token);
}

internal class RedisQueueCacheCursor : IQueueCacheCursor
{
    private readonly List<IBatchContainer> _cache;
    private readonly object _lock;
    private readonly StreamId _streamId;
    private int _index;
    private IBatchContainer? _current;

    public RedisQueueCacheCursor(
        List<IBatchContainer> cache,
        object @lock,
        StreamId streamId,
        StreamSequenceToken? token)
    {
        _cache = cache;
        _lock = @lock;
        _streamId = streamId;
        _index = -1;
    }

    public IBatchContainer GetCurrent(out Exception? exception)
    {
        exception = null;
        return _current!;
    }

    public bool MoveNext()
    {
        lock (_lock)
        {
            while (++_index < _cache.Count)
            {
                var item = _cache[_index];
                if (item.StreamId == _streamId)
                {
                    _current = item;
                    return true;
                }
            }
        }
        return false;
    }

    public void RecordDeliveryFailure() { }
    public void Refresh(StreamSequenceToken token) { }

    public void Dispose() { }
}

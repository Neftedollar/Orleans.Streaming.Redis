using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// In-memory queue cache with watermark-based purging.
///
/// Purge strategy: each cursor tracks the highest index it has consumed.
/// The global low-watermark is the minimum across all active cursors.
/// <see cref="TryPurgeFromCache"/> removes all entries below that watermark,
/// compacting the list and reclaiming memory.
/// </summary>
internal class RedisQueueCache : IQueueCache
{
    private readonly List<IBatchContainer> _cache = [];
    private readonly object _lock = new();
    private readonly int _maxSize;
    private readonly HashSet<RedisQueueCacheCursor> _cursors = [];

    public RedisQueueCache(int maxSize = 4096)
    {
        _maxSize = maxSize;
    }

    public int GetMaxAddCount() => Math.Max(0, _maxSize - _cache.Count);
    public bool IsUnderPressure() => _cache.Count >= _maxSize;

    public void AddToCache(IList<IBatchContainer> messages)
    {
        lock (_lock)
            _cache.AddRange(messages);
    }

    /// <summary>
    /// Purges cache entries that all active cursors have already advanced past.
    /// Returns true (and the removed items) when at least one item was purged.
    /// </summary>
    public bool TryPurgeFromCache(out IList<IBatchContainer>? purgedItems)
    {
        purgedItems = null;

        lock (_lock)
        {
            if (_cache.Count == 0)
                return false;

            // Low-watermark: the minimum "next-to-read" index across all active cursors.
            // A cursor at index i has already delivered everything up to (i - 1).
            int watermark;
            if (_cursors.Count == 0)
            {
                // No active cursors — everything can be purged.
                watermark = _cache.Count;
            }
            else
            {
                watermark = _cursors.Min(c => c.NextIndex);
            }

            if (watermark <= 0)
                return false;

            // Remove the entries [0, watermark) from the front of the cache.
            purgedItems = _cache.GetRange(0, watermark);
            _cache.RemoveRange(0, watermark);

            // Shift all cursor positions down by the number of removed entries.
            foreach (var cursor in _cursors)
                cursor.ShiftBy(watermark);

            return true;
        }
    }

    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken? token)
    {
        lock (_lock)
        {
            var cursor = new RedisQueueCacheCursor(this, _cache, _lock, streamId, token);
            _cursors.Add(cursor);
            return cursor;
        }
    }

    internal void RemoveCursor(RedisQueueCacheCursor cursor)
    {
        lock (_lock)
            _cursors.Remove(cursor);
    }
}

internal class RedisQueueCacheCursor : IQueueCacheCursor
{
    private readonly RedisQueueCache _owner;
    private readonly List<IBatchContainer> _cache;
    private readonly object _lock;
    private readonly StreamId _streamId;

    /// <summary>
    /// Index of the next item to read from _cache.
    /// MoveNext advances this to point past the item it returns.
    /// Exposed so RedisQueueCache can compute the low-watermark.
    /// </summary>
    internal int NextIndex { get; private set; }

    private IBatchContainer? _current;
    private bool _disposed;

    public RedisQueueCacheCursor(
        RedisQueueCache owner,
        List<IBatchContainer> cache,
        object @lock,
        StreamId streamId,
        StreamSequenceToken? token)
    {
        _owner = owner;
        _cache = cache;
        _lock = @lock;
        _streamId = streamId;
        NextIndex = 0;
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
            while (NextIndex < _cache.Count)
            {
                var item = _cache[NextIndex];
                NextIndex++; // advance past this item regardless of stream match
                if (item.StreamId == _streamId)
                {
                    _current = item;
                    return true;
                }
            }
        }
        return false;
    }

    /// <summary>
    /// Adjusts the cursor index after a prefix of the cache list has been removed.
    /// Must be called with the cache lock held.
    /// </summary>
    internal void ShiftBy(int removedCount)
    {
        NextIndex = Math.Max(0, NextIndex - removedCount);
    }

    public void RecordDeliveryFailure() { }
    public void Refresh(StreamSequenceToken token) { }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _owner.RemoveCursor(this);
    }
}

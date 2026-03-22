using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// In-memory queue cache with watermark-based purging.
///
/// Fix #4: O(n) cursor scan replaced with a two-structure layout:
/// - <c>_byStream</c>: Dictionary&lt;StreamId, List&lt;IBatchContainer&gt;&gt; for O(1) per-stream lookup
///   used by cursors (no full scan needed — cursor iterates only its own stream's list).
/// - <c>_ordered</c>: insertion-order list used exclusively by the purge watermark logic.
///
/// Purge strategy: each cursor tracks the highest global index it has consumed.
/// The global low-watermark is the minimum across all active cursors.
/// <see cref="TryPurgeFromCache"/> removes all entries below that watermark,
/// compacting both structures and reclaiming memory.
/// </summary>
internal class RedisQueueCache : IQueueCache
{
    // Insertion-order list for watermark-based purging.
    private readonly List<IBatchContainer> _ordered = [];
    // Per-stream index for O(1) cursor iteration.
    private readonly Dictionary<StreamId, List<IBatchContainer>> _byStream = [];
    private readonly object _lock = new();
    private readonly int _maxSize;
    private readonly HashSet<RedisQueueCacheCursor> _cursors = [];

    public RedisQueueCache(int maxSize = 4096)
    {
        _maxSize = maxSize;
    }

    public int GetMaxAddCount() => Math.Max(0, _maxSize - _ordered.Count);
    public bool IsUnderPressure() => _ordered.Count >= _maxSize;

    public void AddToCache(IList<IBatchContainer> messages)
    {
        lock (_lock)
        {
            foreach (var msg in messages)
            {
                _ordered.Add(msg);
                if (!_byStream.TryGetValue(msg.StreamId, out var list))
                {
                    list = [];
                    _byStream[msg.StreamId] = list;
                }
                list.Add(msg);
            }
        }
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
            if (_ordered.Count == 0)
                return false;

            // Low-watermark: the minimum "next-to-read" global index across all active cursors.
            int watermark;
            if (_cursors.Count == 0)
            {
                watermark = _ordered.Count;
            }
            else
            {
                watermark = _cursors.Min(c => c.NextGlobalIndex);
            }

            if (watermark <= 0)
                return false;

            // Remove [0, watermark) from the ordered list.
            purgedItems = _ordered.GetRange(0, watermark);
            _ordered.RemoveRange(0, watermark);

            // Remove the same entries from the per-stream index.
            foreach (var item in purgedItems)
            {
                if (_byStream.TryGetValue(item.StreamId, out var list))
                {
                    list.Remove(item);
                    if (list.Count == 0)
                        _byStream.Remove(item.StreamId);
                }
            }

            // Shift all cursor global positions down.
            foreach (var cursor in _cursors)
                cursor.ShiftGlobalBy(watermark);

            return true;
        }
    }

    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken? token)
    {
        lock (_lock)
        {
            var cursor = new RedisQueueCacheCursor(this, _byStream, _lock, streamId);
            _cursors.Add(cursor);
            return cursor;
        }
    }

    internal void RemoveCursor(RedisQueueCacheCursor cursor)
    {
        lock (_lock)
            _cursors.Remove(cursor);
    }

    /// <summary>
    /// Returns the current global count of items in the ordered list (for cursor tracking).
    /// Must be called with the lock held.
    /// </summary>
    internal int OrderedCount => _ordered.Count;
}

internal class RedisQueueCacheCursor : IQueueCacheCursor
{
    private readonly RedisQueueCache _owner;
    private readonly Dictionary<StreamId, List<IBatchContainer>> _byStream;
    private readonly object _lock;
    private readonly StreamId _streamId;

    /// <summary>
    /// Index into the stream-specific list (for MoveNext O(1) iteration).
    /// </summary>
    private int _streamIndex;

    /// <summary>
    /// Global insertion-order index — used by the purge watermark calculation.
    /// Tracks how far this cursor has consumed in the global ordered list.
    /// Exposed so RedisQueueCache can compute the low-watermark.
    /// </summary>
    internal int NextGlobalIndex { get; private set; }

    private IBatchContainer? _current;
    private bool _disposed;

    public RedisQueueCacheCursor(
        RedisQueueCache owner,
        Dictionary<StreamId, List<IBatchContainer>> byStream,
        object @lock,
        StreamId streamId)
    {
        _owner = owner;
        _byStream = byStream;
        _lock = @lock;
        _streamId = streamId;
        _streamIndex = 0;
        NextGlobalIndex = 0;
    }

    public IBatchContainer GetCurrent(out Exception? exception)
    {
        exception = null;
        return _current!;
    }

    /// <summary>
    /// Advances to the next item for this stream.
    /// O(1) per call: only iterates the stream-specific list, not the full cache.
    /// Updates <see cref="NextGlobalIndex"/> to the global position of the item consumed.
    /// </summary>
    public bool MoveNext()
    {
        lock (_lock)
        {
            if (!_byStream.TryGetValue(_streamId, out var list))
                return false;

            if (_streamIndex >= list.Count)
                return false;

            _current = list[_streamIndex];
            _streamIndex++;

            // Advance the global watermark pointer by 1.
            NextGlobalIndex++;

            return true;
        }
    }

    /// <summary>
    /// Adjusts the global index after a prefix of the ordered list has been removed.
    /// Also adjusts the per-stream index if items for this stream were purged.
    /// Must be called with the cache lock held.
    /// </summary>
    internal void ShiftGlobalBy(int removedGlobalCount)
    {
        NextGlobalIndex = Math.Max(0, NextGlobalIndex - removedGlobalCount);

        // Recalculate stream index: after purge, some items at the head of the stream
        // list may have been removed. We must not subtract more than the current stream index.
        if (_byStream.TryGetValue(_streamId, out var list))
        {
            // Keep stream index within valid range.
            _streamIndex = Math.Min(_streamIndex, list.Count);
        }
        else
        {
            _streamIndex = 0;
        }
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

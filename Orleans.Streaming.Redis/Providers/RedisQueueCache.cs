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
    ///
    /// Idle cursors (whose stream-specific list is exhausted) do not contribute to
    /// the low-watermark — they would never block purges for other streams' items.
    /// </summary>
    public bool TryPurgeFromCache(out IList<IBatchContainer>? purgedItems)
    {
        purgedItems = null;

        lock (_lock)
        {
            if (_ordered.Count == 0)
                return false;

            // Low-watermark: the minimum effective global index across all active cursors.
            // A cursor is "active" if it has unconsumed items in its stream-specific list.
            // An "idle" cursor (stream empty or exhausted) contributes _ordered.Count so
            // it does not block purges for items it will never consume.
            int watermark;
            if (_cursors.Count == 0)
            {
                watermark = _ordered.Count;
            }
            else
            {
                watermark = _ordered.Count; // start optimistically at end
                foreach (var cursor in _cursors)
                {
                    var effective = cursor.EffectiveWatermark(_byStream, _ordered.Count);
                    if (effective < watermark)
                        watermark = effective;
                }
            }

            if (watermark <= 0)
                return false;

            // Remove [0, watermark) from the ordered list.
            purgedItems = _ordered.GetRange(0, watermark);
            _ordered.RemoveRange(0, watermark);

            // Count how many items are being removed per stream so cursors can
            // adjust their per-stream index correctly.
            var removedPerStream = new Dictionary<StreamId, int>();
            foreach (var item in purgedItems)
            {
                removedPerStream.TryGetValue(item.StreamId, out var n);
                removedPerStream[item.StreamId] = n + 1;

                if (_byStream.TryGetValue(item.StreamId, out var list))
                {
                    list.Remove(item);
                    if (list.Count == 0)
                        _byStream.Remove(item.StreamId);
                }
            }

            // Shift all cursor global positions down, passing per-stream removal counts.
            foreach (var cursor in _cursors)
            {
                removedPerStream.TryGetValue(cursor.StreamId, out var streamRemoved);
                cursor.ShiftGlobalBy(watermark, streamRemoved);
            }

            return true;
        }
    }

    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken? token)
    {
        lock (_lock)
        {
            // Determine the starting stream index: if a token is provided and IsRewindable
            // is true, skip past items whose SequenceToken is strictly less than the
            // requested token so that replay starts at the first item >= token.
            var startIndex = 0;
            if (token is not null && _byStream.TryGetValue(streamId, out var existingList))
            {
                while (startIndex < existingList.Count
                       && existingList[startIndex].SequenceToken.CompareTo(token) < 0)
                {
                    startIndex++;
                }
            }

            var cursor = new RedisQueueCacheCursor(this, _byStream, _lock, streamId, startIndex);
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

    /// <summary>The stream ID this cursor is tracking. Exposed for per-stream purge accounting.</summary>
    internal StreamId StreamId => _streamId;

    /// <summary>
    /// Returns the effective watermark for purge calculation.
    /// If this cursor still has unconsumed items in its stream-specific list, returns
    /// <see cref="NextGlobalIndex"/> (blocks purge at last consumed position).
    /// If the cursor is idle (stream list exhausted or absent), returns
    /// <paramref name="orderedCount"/> so the idle cursor does not block purges.
    /// Must be called with the cache lock held.
    /// </summary>
    internal int EffectiveWatermark(Dictionary<StreamId, List<IBatchContainer>> byStream, int orderedCount)
    {
        if (!byStream.TryGetValue(_streamId, out var list) || _streamIndex >= list.Count)
        {
            // Idle: no more items to consume — don't block purges.
            return orderedCount;
        }
        return NextGlobalIndex;
    }

    private IBatchContainer? _current;
    private bool _disposed;

    public RedisQueueCacheCursor(
        RedisQueueCache owner,
        Dictionary<StreamId, List<IBatchContainer>> byStream,
        object @lock,
        StreamId streamId,
        int startStreamIndex = 0)
    {
        _owner = owner;
        _byStream = byStream;
        _lock = @lock;
        _streamId = streamId;
        _streamIndex = startStreamIndex;
        // NextGlobalIndex must reflect how many global items precede the start position.
        // We approximate by starting at 0; the watermark calculation will still be correct
        // because unread items that come before _streamIndex were already counted by other
        // cursors or are not tracked by this cursor (intentional skip).
        NextGlobalIndex = startStreamIndex;
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
    /// Adjusts the global index and per-stream index after a prefix of the ordered list
    /// has been removed.
    /// </summary>
    /// <param name="removedGlobalCount">Number of items removed from the global ordered list.</param>
    /// <param name="removedStreamCount">
    /// Number of items removed from this cursor's stream-specific list.
    /// The cursor's <c>_streamIndex</c> must be decremented by this amount so it
    /// continues to point at the correct next-to-deliver item after items that
    /// have already been consumed are purged from the head of the stream list.
    /// </param>
    internal void ShiftGlobalBy(int removedGlobalCount, int removedStreamCount = 0)
    {
        NextGlobalIndex = Math.Max(0, NextGlobalIndex - removedGlobalCount);

        // Adjust the per-stream index: items that were already delivered (before _streamIndex)
        // may have been purged from the head of the stream list. Decrement _streamIndex by the
        // number of removed items to keep it pointing at the correct next item.
        _streamIndex = Math.Max(0, _streamIndex - removedStreamCount);

        // Safety clamp: don't exceed the current list length.
        if (_byStream.TryGetValue(_streamId, out var list))
        {
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

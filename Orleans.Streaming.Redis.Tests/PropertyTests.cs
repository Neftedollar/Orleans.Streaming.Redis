// Suppress NUnit1027: FsCheck [Property] attribute handles test parameters differently from NUnit.
#pragma warning disable NUnit1027

using FsCheck;
using FsCheck.Fluent;
using FsCheck.NUnit;
using Orleans.Providers.Streams.Common;
using Orleans.Streaming.Redis.Providers;
using Orleans.Streams;

namespace Orleans.Streaming.Redis.Tests;

/// <summary>
/// FsCheck property-based tests for Orleans.Streaming.Redis core invariants.
/// Uses FsCheck 3.x with [FsCheck.NUnit.Property] attribute.
/// </summary>
public class PropertyTests
{
    // -------------------------------------------------------------------------
    // 1. StableHash determinism
    // Property: StableHash(s) always returns the same result for the same input.
    // -------------------------------------------------------------------------

    [FsCheck.NUnit.Property(MaxTest = 500)]
    public bool StableHash_IsDeterministic(NonNull<string> input)
    {
        var s = input.Get;
        var h1 = RedisStreamQueueMapper.StableHash(s);
        var h2 = RedisStreamQueueMapper.StableHash(s);
        return h1 == h2;
    }

    // -------------------------------------------------------------------------
    // 2. QueueMapper consistency
    // Property: GetQueueForStream for the same StreamId always returns the same QueueId.
    // -------------------------------------------------------------------------

    [FsCheck.NUnit.Property(MaxTest = 500)]
    public bool QueueMapper_IsConsistent(NonNull<string> ns, NonNull<string> key)
    {
        var mapper = new RedisStreamQueueMapper(8, "test");
        var streamId = StreamId.Create(ns.Get, key.Get);
        var q1 = mapper.GetQueueForStream(streamId);
        var q2 = mapper.GetQueueForStream(streamId);
        return q1 == q2;
    }

    // -------------------------------------------------------------------------
    // 3. QueueMapper distribution
    // Property: for 200 random StreamIds, no single partition receives > 50% of messages.
    // Catastrophic clustering would mean all messages hit one shard.
    // -------------------------------------------------------------------------

    [FsCheck.NUnit.Property(MaxTest = 50)]
    public Property QueueMapper_NoCatastrophicClustering(int seed)
    {
        var mapper = new RedisStreamQueueMapper(8, "test");
        var rng = new System.Random(seed);
        var counts = new Dictionary<QueueId, int>();

        const int total = 200;
        for (var i = 0; i < total; i++)
        {
            var key = $"stream-{rng.Next(100_000)}";
            var streamId = StreamId.Create("ns", key);
            var q = mapper.GetQueueForStream(streamId);
            counts.TryGetValue(q, out var c);
            counts[q] = c + 1;
        }

        var maxLoad = counts.Values.Max();
        var maxFraction = (double)maxLoad / total;

        return Prop.Label(maxFraction < 0.50,
            $"Max partition load was {maxFraction:P1} (seed={seed}), expected < 50%");
    }

    // -------------------------------------------------------------------------
    // 4. Sequence token ordering preservation
    // Property: if timestamp1 < timestamp2 OR (timestamp1 == timestamp2 AND seq1 < seq2),
    // then the parsed token for id1 compares-less-than token for id2.
    // -------------------------------------------------------------------------

    [FsCheck.NUnit.Property(MaxTest = 500)]
    public Property SequenceToken_OrderingPreserved(
        PositiveInt ts1, NonNegativeInt seq1,
        PositiveInt ts2, NonNegativeInt seq2)
    {
        var timestamp1 = (long)ts1.Get;
        var sequence1 = seq1.Get;
        var timestamp2 = (long)ts2.Get;
        var sequence2 = seq2.Get;

        // Only test pairs where a strict ordering is defined.
        var id1StrictlyLess =
            timestamp1 < timestamp2 ||
            (timestamp1 == timestamp2 && sequence1 < sequence2);

        if (!id1StrictlyLess)
            return Prop.ToProperty(true); // precondition not met — skip

        var entryId1 = $"{timestamp1}-{sequence1}";
        var entryId2 = $"{timestamp2}-{sequence2}";

        var token1 = RedisStreamReceiver.ParseRedisEntryId(entryId1);
        var token2 = RedisStreamReceiver.ParseRedisEntryId(entryId2);

        var cmp = token1.CompareTo(token2);

        return Prop.Label(cmp < 0,
            $"token1({entryId1}).CompareTo(token2({entryId2})) = {cmp}, expected < 0");
    }

    // -------------------------------------------------------------------------
    // 5. Serialization roundtrip via RedisBatchContainer.GetEvents<T>
    // Property: events placed into a RedisBatchContainer round-trip through GetEvents<string>.
    // -------------------------------------------------------------------------

    [FsCheck.NUnit.Property(MaxTest = 500)]
    public bool BatchContainer_RoundtripsEvents(NonEmptyString input)
    {
        var value = input.Get;
        var streamId = StreamId.Create("ns", "key");
        var token = new EventSequenceTokenV2(1);
        var events = new List<object> { value };
        var container = new RedisBatchContainer(streamId, events, null, token);

        var retrieved = container.GetEvents<string>().Select(t => t.Item1).ToList();

        return retrieved.Count == 1 && retrieved[0] == value;
    }

    // -------------------------------------------------------------------------
    // 6. Model-based cache testing
    // Property: for any random sequence of cache operations, core invariants hold:
    //   (a) GetMaxAddCount() >= 0
    //   (b) Cursor.MoveNext only returns items for the cursor's own StreamId
    // -------------------------------------------------------------------------

    private enum CacheOp { Add, Purge, CreateCursor, MoveCursor, DisposeCursor }

    [FsCheck.NUnit.Property(MaxTest = 200)]
    public Property Cache_InvariantsHoldUnderRandomOperations(
        NonEmptyArray<byte> opCodes,
        PositiveInt maxSizeRaw)
    {
        var maxSize = (maxSizeRaw.Get % 50) + 5; // keep maxSize in [5, 54]
        var cache = new RedisQueueCache(maxSize);

        var streamIdA = StreamId.Create("ns", "a");
        var streamIdB = StreamId.Create("ns", "b");
        long seq = 0;

        // Track cursors so we can randomly advance or dispose them.
        var cursors = new List<(IQueueCacheCursor cursor, StreamId streamId)>();
        var violations = new List<string>();

        foreach (var code in opCodes.Get)
        {
            var op = (CacheOp)(code % 5);

            switch (op)
            {
                case CacheOp.Add:
                    var addCount = cache.GetMaxAddCount();
                    if (addCount > 0)
                    {
                        var toAdd = Math.Min(addCount, 3);
                        var msgs = new List<IBatchContainer>();
                        for (var i = 0; i < toAdd; i++)
                        {
                            seq++;
                            var sid = (seq % 2 == 0) ? streamIdA : streamIdB;
                            var t = new EventSequenceTokenV2(seq);
                            msgs.Add(new RedisBatchContainer(sid, [$"evt-{seq}"], null, t));
                        }
                        cache.AddToCache(msgs);
                    }
                    break;

                case CacheOp.Purge:
                    cache.TryPurgeFromCache(out _);
                    break;

                case CacheOp.CreateCursor:
                    // Limit cursor count to avoid unbounded growth in test.
                    if (cursors.Count < 8)
                    {
                        var sid = (cursors.Count % 2 == 0) ? streamIdA : streamIdB;
                        var cursor = cache.GetCacheCursor(sid, null);
                        cursors.Add((cursor, sid));
                    }
                    break;

                case CacheOp.MoveCursor:
                    if (cursors.Count > 0)
                    {
                        var idx = code % cursors.Count;
                        var (cursor, sid) = cursors[idx];
                        if (cursor.MoveNext())
                        {
                            var item = cursor.GetCurrent(out _);
                            // Invariant (b): cursor only returns items for its own stream.
                            if (item.StreamId != sid)
                                violations.Add($"Cursor for {sid} returned item for {item.StreamId}");
                        }
                    }
                    break;

                case CacheOp.DisposeCursor:
                    if (cursors.Count > 0)
                    {
                        var idx = code % cursors.Count;
                        cursors[idx].cursor.Dispose();
                        cursors.RemoveAt(idx);
                    }
                    break;
            }

            // Invariant (a): GetMaxAddCount() >= 0
            var mac = cache.GetMaxAddCount();
            if (mac < 0)
                violations.Add($"GetMaxAddCount() returned {mac} < 0");
        }

        // Dispose remaining cursors.
        foreach (var (c, _) in cursors)
            c.Dispose();

        return Prop.Label(
            violations.Count == 0,
            violations.Count > 0
                ? $"Violations: {string.Join("; ", violations)}"
                : "All invariants hold");
    }
}

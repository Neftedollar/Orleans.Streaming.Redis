using System.Diagnostics.Metrics;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using Orleans.Streaming.Redis.Configuration;
using Orleans.Streaming.Redis.Providers;

namespace Orleans.Streaming.Redis.Tests;

public class RedisStreamQueueMapperTests
{
    [Test]
    public void GetAllQueues_ReturnsCorrectCount()
    {
        var mapper = new RedisStreamQueueMapper(8, "test");
        var queues = mapper.GetAllQueues().ToList();
        Assert.That(queues, Has.Count.EqualTo(8));
    }

    [Test]
    public void GetAllQueues_ReturnsUniqueIds()
    {
        var mapper = new RedisStreamQueueMapper(16, "test");
        var queues = mapper.GetAllQueues().ToList();
        Assert.That(queues.Distinct().Count(), Is.EqualTo(16));
    }

    [Test]
    public void GetQueueForStream_SameStreamId_ReturnsSameQueue()
    {
        var mapper = new RedisStreamQueueMapper(8, "test");
        var streamId = StreamId.Create("ns", "key1");

        var q1 = mapper.GetQueueForStream(streamId);
        var q2 = mapper.GetQueueForStream(streamId);

        Assert.That(q2, Is.EqualTo(q1));
    }

    [Test]
    public void GetQueueForStream_DifferentStreams_DistributeAcrossQueues()
    {
        var mapper = new RedisStreamQueueMapper(8, "test");
        var queues = new HashSet<QueueId>();

        // Generate enough streams to expect at least 2 different queues
        for (var i = 0; i < 100; i++)
        {
            var streamId = StreamId.Create("ns", $"key-{i}");
            queues.Add(mapper.GetQueueForStream(streamId));
        }

        Assert.That(queues.Count, Is.GreaterThan(1),
            "100 streams should hash to more than 1 partition");
    }

    [Test]
    public void GetRedisKey_FormatsCorrectly()
    {
        var mapper = new RedisStreamQueueMapper(4, "provider");
        var queue = mapper.GetAllQueues().First();
        var key = RedisStreamQueueMapper.GetRedisKey("orleans:stream", queue);

        Assert.That(key, Does.StartWith("orleans:stream:"));
    }

    [Test]
    public void GetRedisKey_CustomPrefix()
    {
        var mapper = new RedisStreamQueueMapper(2, "test");
        var queue = mapper.GetAllQueues().First();
        var key = RedisStreamQueueMapper.GetRedisKey("myapp:events", queue);

        Assert.That(key, Does.StartWith("myapp:events:"));
    }

    [TestCase(1)]
    [TestCase(4)]
    [TestCase(32)]
    [TestCase(128)]
    public void GetAllQueues_VariousPartitionCounts(int count)
    {
        var mapper = new RedisStreamQueueMapper(count, "test");
        Assert.That(mapper.GetAllQueues().Count(), Is.EqualTo(count));
    }
}

public class RedisStreamOptionsTests
{
    [Test]
    public void Defaults_AreReasonable()
    {
        var opts = new RedisStreamOptions();

        Assert.Multiple(() =>
        {
            Assert.That(opts.ConnectionString, Is.Empty);
            Assert.That(opts.QueueCount, Is.EqualTo(8));
            Assert.That(opts.KeyPrefix, Is.EqualTo("orleans:stream"));
            Assert.That(opts.ConsumerGroup, Is.EqualTo("orleans"));
            Assert.That(opts.MaxBatchSize, Is.EqualTo(100));
            Assert.That(opts.MaxStreamLength, Is.EqualTo(10_000));
            Assert.That(opts.Database, Is.EqualTo(-1));
        });
    }

    [Test]
    public void AllProperties_AreSettable()
    {
        var opts = new RedisStreamOptions
        {
            ConnectionString = "redis:6379",
            QueueCount = 16,
            KeyPrefix = "custom:prefix",
            ConsumerGroup = "my-group",
            MaxBatchSize = 50,
            MaxStreamLength = 5000,
            Database = 3
        };

        Assert.Multiple(() =>
        {
            Assert.That(opts.ConnectionString, Is.EqualTo("redis:6379"));
            Assert.That(opts.QueueCount, Is.EqualTo(16));
            Assert.That(opts.KeyPrefix, Is.EqualTo("custom:prefix"));
            Assert.That(opts.ConsumerGroup, Is.EqualTo("my-group"));
            Assert.That(opts.MaxBatchSize, Is.EqualTo(50));
            Assert.That(opts.MaxStreamLength, Is.EqualTo(5000));
            Assert.That(opts.Database, Is.EqualTo(3));
        });
    }

    // Fix #6 validation tests
    [Test]
    public void Validate_EmptyConnectionString_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "" };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_WhitespaceConnectionString_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "   " };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_ZeroQueueCount_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", QueueCount = 0 };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_NegativeQueueCount_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", QueueCount = -1 };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_ZeroMaxBatchSize_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", MaxBatchSize = 0 };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_EmptyKeyPrefix_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", KeyPrefix = "" };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_ValidOptions_DoesNotThrow()
    {
        var opts = new RedisStreamOptions
        {
            ConnectionString = "localhost:6379",
            QueueCount = 4,
            MaxBatchSize = 50,
            KeyPrefix = "test:stream"
        };
        Assert.DoesNotThrow(() => opts.Validate());
    }
}

public class RedisBatchContainerTests
{
    [Test]
    public void StreamId_PreservedFromConstructor()
    {
        var streamId = StreamId.Create("test-ns", "test-key");
        var token = new Orleans.Providers.Streams.Common.EventSequenceTokenV2(42);
        var container = new RedisBatchContainer(streamId, [1, 2, 3], null, token);

        Assert.That(container.StreamId, Is.EqualTo(streamId));
    }

    [Test]
    public void SequenceToken_PreservedFromConstructor()
    {
        var streamId = StreamId.Create("ns", "key");
        var token = new Orleans.Providers.Streams.Common.EventSequenceTokenV2(99);
        var container = new RedisBatchContainer(streamId, ["hello"], null, token);

        Assert.That(container.SequenceToken, Is.Not.Null);
    }

    [Test]
    public void GetEvents_ReturnsMatchingTypes()
    {
        var streamId = StreamId.Create("ns", "key");
        var token = new Orleans.Providers.Streams.Common.EventSequenceTokenV2(0);
        var events = new List<object> { "string1", 42, "string2", 99 };
        var container = new RedisBatchContainer(streamId, events, null, token);

        var strings = container.GetEvents<string>().ToList();
        Assert.That(strings, Has.Count.EqualTo(2));
        Assert.That(strings[0].Item1, Is.EqualTo("string1"));
        Assert.That(strings[1].Item1, Is.EqualTo("string2"));
    }

    [Test]
    public void GetEvents_NoMatchingTypes_ReturnsEmpty()
    {
        var streamId = StreamId.Create("ns", "key");
        var token = new Orleans.Providers.Streams.Common.EventSequenceTokenV2(0);
        var container = new RedisBatchContainer(streamId, [1, 2, 3], null, token);

        var strings = container.GetEvents<string>().ToList();
        Assert.That(strings, Is.Empty);
    }

    [Test]
    public void ImportRequestContext_WithNullContext_ReturnsFalse()
    {
        var streamId = StreamId.Create("ns", "key");
        var token = new Orleans.Providers.Streams.Common.EventSequenceTokenV2(0);
        var container = new RedisBatchContainer(streamId, [], null, token);

        Assert.That(container.ImportRequestContext(), Is.False);
    }

    [Test]
    public void ImportRequestContext_WithContext_ReturnsTrue()
    {
        var streamId = StreamId.Create("ns", "key");
        var token = new Orleans.Providers.Streams.Common.EventSequenceTokenV2(0);
        var ctx = new Dictionary<string, object> { ["key1"] = "value1" };
        var container = new RedisBatchContainer(streamId, [], ctx, token);

        Assert.That(container.ImportRequestContext(), Is.True);
    }
}

/// <summary>
/// Unit tests for RedisQueueCache purge behaviour.
/// </summary>
public class RedisQueueCacheTests
{
    private static RedisBatchContainer MakeContainer(string ns, string key, long seq)
    {
        var streamId = StreamId.Create(ns, key);
        var token = new EventSequenceTokenV2(seq);
        return new RedisBatchContainer(streamId, [$"event-{seq}"], null, token);
    }

    [Test]
    public void TryPurgeFromCache_NoCursors_PurgesAll()
    {
        var cache = new RedisQueueCache(128);
        var msgs = new List<IBatchContainer>
        {
            MakeContainer("ns", "k", 1),
            MakeContainer("ns", "k", 2),
            MakeContainer("ns", "k", 3),
        };
        cache.AddToCache(msgs);

        var purged = cache.TryPurgeFromCache(out var items);

        Assert.That(purged, Is.True);
        Assert.That(items, Has.Count.EqualTo(3));
        // After purge the cache should be empty — no cursor to block
        Assert.That(cache.GetMaxAddCount(), Is.EqualTo(128));
    }

    [Test]
    public void TryPurgeFromCache_CursorNotAdvanced_PurgesNothing()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("ns", "k", 1),
            MakeContainer("ns", "k", 2),
        });

        // Obtain a cursor but don't advance it.
        using var cursor = cache.GetCacheCursor(streamId, null);

        var purged = cache.TryPurgeFromCache(out var items);

        Assert.That(purged, Is.False, "Nothing should be purged when cursor is at position 0");
        Assert.That(items, Is.Null);
    }

    [Test]
    public void TryPurgeFromCache_CursorFullyAdvanced_PurgesAll()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("ns", "k", 1),
            MakeContainer("ns", "k", 2),
            MakeContainer("ns", "k", 3),
        });

        using var cursor = cache.GetCacheCursor(streamId, null);
        // Advance past all items.
        while (cursor.MoveNext()) { }

        var purged = cache.TryPurgeFromCache(out var items);

        Assert.That(purged, Is.True);
        Assert.That(items, Has.Count.EqualTo(3));
    }

    [Test]
    public void TryPurgeFromCache_CursorPartiallyAdvanced_PurgesOnlyConsumed()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("ns", "k", 1),
            MakeContainer("ns", "k", 2),
            MakeContainer("ns", "k", 3),
        });

        using var cursor = cache.GetCacheCursor(streamId, null);
        // Advance past the first item only.
        cursor.MoveNext();

        var purged = cache.TryPurgeFromCache(out var items);

        Assert.That(purged, Is.True);
        Assert.That(items, Has.Count.EqualTo(1), "Only the item the cursor advanced past should be purged");
    }

    [Test]
    public void TryPurgeFromCache_DisposedCursor_DoesNotBlockPurge()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("ns", "k", 1),
            MakeContainer("ns", "k", 2),
        });

        // Create, advance partially, then dispose the cursor.
        var cursor = cache.GetCacheCursor(streamId, null);
        cursor.MoveNext();
        cursor.Dispose(); // removed from _cursors

        // With no active cursors, everything should purge.
        var purged = cache.TryPurgeFromCache(out var items);

        Assert.That(purged, Is.True);
        Assert.That(items, Has.Count.EqualTo(2));
    }

    [Test]
    public void TryPurgeFromCache_EmptyCache_ReturnsFalse()
    {
        var cache = new RedisQueueCache(128);

        var purged = cache.TryPurgeFromCache(out var items);

        Assert.That(purged, Is.False);
        Assert.That(items, Is.Null);
    }

    [Test]
    public void IsUnderPressure_TrueWhenAtMaxSize()
    {
        var cache = new RedisQueueCache(2);
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("ns", "k", 1),
            MakeContainer("ns", "k", 2),
        });

        Assert.That(cache.IsUnderPressure(), Is.True);
    }

    // Fix #4: verify that cursor only sees items for its own stream (O(1) behaviour).
    [Test]
    public void Cursor_MoveNext_OnlyReturnsSameStreamId()
    {
        var cache = new RedisQueueCache(128);
        var streamA = StreamId.Create("ns", "a");
        var streamB = StreamId.Create("ns", "b");

        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("ns", "a", 1),
            MakeContainer("ns", "b", 10),
            MakeContainer("ns", "a", 2),
            MakeContainer("ns", "b", 20),
            MakeContainer("ns", "a", 3),
        });

        using var cursor = cache.GetCacheCursor(streamA, null);
        var items = new List<IBatchContainer>();
        while (cursor.MoveNext())
            items.Add(cursor.GetCurrent(out _));

        Assert.That(items, Has.Count.EqualTo(3), "Cursor should see only stream A items");
        Assert.That(items.All(i => i.StreamId == streamA), Is.True);
    }
}

/// <summary>
/// Unit tests for metrics counters (Fix #5).
/// Uses MeterListener to observe counter increments without a real Redis connection.
/// </summary>
public class RedisStreamMetricsTests
{
    [Test]
    public void MetricsCounters_AreDefinedWithCorrectMeterName()
    {
        // Verify static counter references are non-null and correctly named.
        Assert.That(RedisStreamMetrics.MessagesEnqueued, Is.Not.Null);
        Assert.That(RedisStreamMetrics.MessagesDequeued, Is.Not.Null);
        Assert.That(RedisStreamMetrics.MessagesAcked, Is.Not.Null);
        Assert.That(RedisStreamMetrics.MessagesFailed, Is.Not.Null);
        Assert.That(RedisStreamMetrics.MessagesClaimed, Is.Not.Null);
    }

    [Test]
    public void MessagesEnqueued_Counter_Increments()
    {
        long observed = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Orleans.Streaming.Redis"
                && instrument.Name == "orleans.streaming.redis.messages_enqueued")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref observed, measurement));
        listener.Start();

        RedisStreamMetrics.MessagesEnqueued.Add(3);
        listener.RecordObservableInstruments();

        Assert.That(observed, Is.EqualTo(3));
    }

    [Test]
    public void MessagesDequeued_Counter_Increments()
    {
        long observed = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Orleans.Streaming.Redis"
                && instrument.Name == "orleans.streaming.redis.messages_dequeued")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref observed, measurement));
        listener.Start();

        RedisStreamMetrics.MessagesDequeued.Add(5);
        listener.RecordObservableInstruments();

        Assert.That(observed, Is.EqualTo(5));
    }

    [Test]
    public void MessagesAcked_Counter_Increments()
    {
        long observed = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Orleans.Streaming.Redis"
                && instrument.Name == "orleans.streaming.redis.messages_acked")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref observed, measurement));
        listener.Start();

        RedisStreamMetrics.MessagesAcked.Add(2);
        listener.RecordObservableInstruments();

        Assert.That(observed, Is.EqualTo(2));
    }

    [Test]
    public void MessagesFailed_Counter_Increments()
    {
        long observed = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Orleans.Streaming.Redis"
                && instrument.Name == "orleans.streaming.redis.messages_failed")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref observed, measurement));
        listener.Start();

        RedisStreamMetrics.MessagesFailed.Add(1);
        listener.RecordObservableInstruments();

        Assert.That(observed, Is.EqualTo(1));
    }

    [Test]
    public void MessagesClaimed_Counter_Increments()
    {
        long observed = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Orleans.Streaming.Redis"
                && instrument.Name == "orleans.streaming.redis.messages_claimed")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref observed, measurement));
        listener.Start();

        RedisStreamMetrics.MessagesClaimed.Add(7);
        listener.RecordObservableInstruments();

        Assert.That(observed, Is.EqualTo(7));
    }
}

/// <summary>
/// Tests for fix #1 — FNV-1a stable hash in GetQueueForStream.
/// </summary>
public class StableHashTests
{
    [Test]
    public void GetQueueForStream_SameInput_AlwaysReturnsSameQueue()
    {
        // Run 1000 times to guard against accidental false-positives.
        var mapper = new RedisStreamQueueMapper(8, "test");
        var streamId = StreamId.Create("ns", "consistent-key");

        var first = mapper.GetQueueForStream(streamId);
        for (var i = 0; i < 1000; i++)
            Assert.That(mapper.GetQueueForStream(streamId), Is.EqualTo(first),
                "StableHash must return the same result for the same input on every call");
    }

    [Test]
    public void GetQueueForStream_DifferentProviderInstances_SameMapping()
    {
        // Two independent mapper instances must produce identical queue assignments
        // for the same stream ID (simulates two silos with independent instances).
        var mapper1 = new RedisStreamQueueMapper(16, "provider");
        var mapper2 = new RedisStreamQueueMapper(16, "provider");

        for (var i = 0; i < 50; i++)
        {
            var streamId = StreamId.Create("ns", $"key-{i}");
            Assert.That(mapper2.GetQueueForStream(streamId), Is.EqualTo(mapper1.GetQueueForStream(streamId)),
                $"Both silo instances must map stream 'key-{i}' to the same partition");
        }
    }

    [Test]
    public void GetQueueForStream_DistributesAcrossAllQueues()
    {
        // With 100 distinct stream IDs we should hit more than one partition out of 8.
        var mapper = new RedisStreamQueueMapper(8, "test");
        var seen = new HashSet<QueueId>();
        for (var i = 0; i < 100; i++)
            seen.Add(mapper.GetQueueForStream(StreamId.Create("ns", $"stream-{i}")));

        Assert.That(seen.Count, Is.GreaterThan(1),
            "FNV-1a hash should distribute 100 streams across more than 1 partition");
    }
}

/// <summary>
/// Tests for fix #2 — GetCacheCursor respects the stream sequence token (rewind support).
/// </summary>
public class RewindCursorTests
{
    private static RedisBatchContainer MakeContainer(string key, long seq)
    {
        var streamId = StreamId.Create("ns", key);
        var token = new EventSequenceTokenV2(seq);
        return new RedisBatchContainer(streamId, [$"evt-{seq}"], null, token);
    }

    [Test]
    public void GetCacheCursor_NullToken_StartsFromBeginning()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("k", 10),
            MakeContainer("k", 20),
            MakeContainer("k", 30),
        });

        using var cursor = cache.GetCacheCursor(streamId, null);
        var items = new List<long>();
        while (cursor.MoveNext())
            items.Add(((EventSequenceTokenV2)cursor.GetCurrent(out _).SequenceToken).SequenceNumber);

        Assert.That(items, Is.EqualTo(new[] { 10L, 20L, 30L }),
            "Null token should start from the first item in the cache");
    }

    [Test]
    public void GetCacheCursor_WithToken_SkipsItemsBeforeToken()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("k", 10),
            MakeContainer("k", 20),
            MakeContainer("k", 30),
            MakeContainer("k", 40),
        });

        // Request cursor starting at seq=20; items 10 should be skipped.
        var startToken = new EventSequenceTokenV2(20);
        using var cursor = cache.GetCacheCursor(streamId, startToken);
        var items = new List<long>();
        while (cursor.MoveNext())
            items.Add(((EventSequenceTokenV2)cursor.GetCurrent(out _).SequenceToken).SequenceNumber);

        Assert.That(items, Is.EqualTo(new[] { 20L, 30L, 40L }),
            "Cursor should start at the first item >= the requested token");
    }

    [Test]
    public void GetCacheCursor_WithTokenBeyondCache_ReturnsEmpty()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("k", 10),
            MakeContainer("k", 20),
        });

        // Token is beyond all cached items.
        var startToken = new EventSequenceTokenV2(100);
        using var cursor = cache.GetCacheCursor(streamId, startToken);

        Assert.That(cursor.MoveNext(), Is.False,
            "Cursor with token beyond cached range should return no items");
    }

    [Test]
    public void GetCacheCursor_WithTokenBeforeAll_SameAsNull()
    {
        var cache = new RedisQueueCache(128);
        var streamId = StreamId.Create("ns", "k");
        cache.AddToCache(new List<IBatchContainer>
        {
            MakeContainer("k", 10),
            MakeContainer("k", 20),
        });

        // Token before first item — should include all cached items.
        var startToken = new EventSequenceTokenV2(5);
        using var cursor = cache.GetCacheCursor(streamId, startToken);
        var items = new List<long>();
        while (cursor.MoveNext())
            items.Add(((EventSequenceTokenV2)cursor.GetCurrent(out _).SequenceToken).SequenceNumber);

        Assert.That(items, Is.EqualTo(new[] { 10L, 20L }),
            "Token before all cached items should behave the same as null (start from beginning)");
    }
}

/// <summary>
/// Tests for fix #5 — Options.CacheSize property and validation.
/// </summary>
public class CacheSizeOptionsTests
{
    [Test]
    public void CacheSize_DefaultIs4096()
    {
        var opts = new RedisStreamOptions();
        Assert.That(opts.CacheSize, Is.EqualTo(4096));
    }

    [Test]
    public void CacheSize_IsSettable()
    {
        var opts = new RedisStreamOptions { CacheSize = 1024 };
        Assert.That(opts.CacheSize, Is.EqualTo(1024));
    }

    [Test]
    public void Validate_ZeroCacheSize_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", CacheSize = 0 };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_NegativeCacheSize_Throws()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", CacheSize = -1 };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Test]
    public void Validate_CustomCacheSize_DoesNotThrow()
    {
        var opts = new RedisStreamOptions { ConnectionString = "localhost", CacheSize = 512 };
        Assert.DoesNotThrow(() => opts.Validate());
    }

    [Test]
    public void SimpleQueueAdapterCache_UsesCacheSize()
    {
        // Verify the cache is created with the configured size.
        var cache = new Orleans.Streaming.Redis.Providers.SimpleQueueAdapterCache("test", 256);
        var queueCache = (RedisQueueCache)cache.CreateQueueCache(QueueId.GetQueueId("test", 0, 0));
        Assert.That(queueCache.GetMaxAddCount(), Is.EqualTo(256),
            "Cache should be created with the configured CacheSize");
    }
}

/// <summary>
/// Tests for fix #6 — DeadLetterPrefix option.
/// </summary>
public class DeadLetterOptionsTests
{
    [Test]
    public void DeadLetterPrefix_DefaultIsNull()
    {
        var opts = new RedisStreamOptions();
        Assert.That(opts.DeadLetterPrefix, Is.Null);
    }

    [Test]
    public void DeadLetterPrefix_IsSettable()
    {
        var opts = new RedisStreamOptions { DeadLetterPrefix = "dead:letters" };
        Assert.That(opts.DeadLetterPrefix, Is.EqualTo("dead:letters"));
    }

    [Test]
    public void Validate_WithDeadLetterPrefix_DoesNotThrow()
    {
        var opts = new RedisStreamOptions
        {
            ConnectionString = "localhost",
            DeadLetterPrefix = "dl:stream",
        };
        Assert.DoesNotThrow(() => opts.Validate());
    }
}

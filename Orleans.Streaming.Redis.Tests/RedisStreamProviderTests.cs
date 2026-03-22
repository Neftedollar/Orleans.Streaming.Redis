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
}

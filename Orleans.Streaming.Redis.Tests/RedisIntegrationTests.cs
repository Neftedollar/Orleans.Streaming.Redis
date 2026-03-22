using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streaming.Redis.Configuration;
using Orleans.Streaming.Redis.Providers;
using Orleans.Streams;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Orleans.Streaming.Redis.Tests;

/// <summary>
/// Integration tests against a real Redis instance via Testcontainers.
/// Tests the full XADD → XREADGROUP → XACK cycle.
/// </summary>
[TestFixture]
public class RedisIntegrationTests
{
    private RedisContainer _redis = null!;
    private IConnectionMultiplexer _connection = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _redis = new RedisBuilder().Build();
        await _redis.StartAsync();
        _connection = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());

        // Build Orleans Serializer — required for payload serialization.
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        _serializer = sp.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _connection?.Dispose();
        await _redis.DisposeAsync();
    }

    [Test]
    public async Task Adapter_QueueAndReceive_RoundTrip()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 2,
            KeyPrefix = "test:roundtrip",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(options.QueueCount, "test");
        var adapter = new RedisStreamAdapter("test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "key1");

        // Produce
        await adapter.QueueMessageBatchAsync(streamId, new[] { "hello", "world" }, null, null);

        // Consume
        var queueId = mapper.GetQueueForStream(streamId);
        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        var messages = await receiver.GetQueueMessagesAsync(10);

        Assert.That(messages, Has.Count.EqualTo(1));
        var container = (RedisBatchContainer)messages[0];
        Assert.That(container.StreamId, Is.EqualTo(streamId));

        var events = container.GetEvents<string>().ToList();
        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events[0].Item1, Is.EqualTo("hello"));
        Assert.That(events[1].Item1, Is.EqualTo("world"));
    }

    [Test]
    public async Task Receiver_XACK_AcknowledgesMessages()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:xack",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "xack-test");
        var adapter = new RedisStreamAdapter("xack-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "ack-key");
        await adapter.QueueMessageBatchAsync(streamId, new[] { "msg1" }, null, null);

        var queueId = mapper.GetAllQueues().First();
        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        var messages = await receiver.GetQueueMessagesAsync(10);
        Assert.That(messages, Has.Count.EqualTo(1));

        // Verify entry ID is captured
        var rbc = (RedisBatchContainer)messages[0];
        Assert.That(rbc.RedisEntryId, Is.Not.Null.And.Not.Empty);

        // ACK the messages
        await receiver.MessagesDeliveredAsync(messages);

        // Verify: XPENDING should show 0 pending for this group
        var db = _connection.GetDatabase();
        var streamKey = RedisStreamQueueMapper.GetRedisKey(options.KeyPrefix, queueId);
        var pending = await db.StreamPendingAsync(streamKey, options.ConsumerGroup);
        Assert.That(pending.PendingMessageCount, Is.EqualTo(0),
            "After XACK, no messages should be pending");
    }

    [Test]
    public async Task Receiver_EmptyStream_ReturnsEmptyList()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:empty",
        };
        var mapper = new RedisStreamQueueMapper(1, "empty-test");
        var adapter = new RedisStreamAdapter("empty-test", options, _connection, _serializer, mapper);

        var queueId = mapper.GetAllQueues().First();
        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        var messages = await receiver.GetQueueMessagesAsync(10);
        Assert.That(messages, Is.Empty);
    }

    [Test]
    public async Task Adapter_MaxStreamLength_CapsStreamSize()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:maxlen",
            MaxStreamLength = 50,
        };
        var mapper = new RedisStreamQueueMapper(1, "maxlen-test");
        var adapter = new RedisStreamAdapter("maxlen-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "maxlen-key");

        // Write 200 messages — stream should be capped at ~50 (approximate MAXLEN)
        for (var i = 0; i < 200; i++)
            await adapter.QueueMessageBatchAsync(streamId, new[] { $"msg-{i}" }, null, null);

        var db = _connection.GetDatabase();
        var queueId = mapper.GetAllQueues().First();
        var streamKey = RedisStreamQueueMapper.GetRedisKey(options.KeyPrefix, queueId);
        var length = await db.StreamLengthAsync(streamKey);

        // Approximate MAXLEN (~) — Redis may keep more than the exact limit,
        // but with 200 entries and MAXLEN ~50, it should be well below 200.
        Assert.That(length, Is.LessThan(150),
            "Stream should be trimmed significantly below the total number of messages written");
    }

    [Test]
    public async Task Adapter_MultiplePartitions_DistributeMessages()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 4,
            KeyPrefix = "test:partitions",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(4, "part-test");
        var adapter = new RedisStreamAdapter("part-test", options, _connection, _serializer, mapper);

        // Write messages with different stream IDs
        for (var i = 0; i < 20; i++)
        {
            var streamId = StreamId.Create("ns", $"part-key-{i}");
            await adapter.QueueMessageBatchAsync(streamId, new[] { $"msg-{i}" }, null, null);
        }

        // Read from all partitions
        var totalMessages = 0;
        var partitionsWithMessages = 0;
        foreach (var queueId in mapper.GetAllQueues())
        {
            var receiver = adapter.CreateReceiver(queueId);
            await receiver.Initialize(TimeSpan.FromSeconds(5));
            var messages = await receiver.GetQueueMessagesAsync(100);
            totalMessages += messages.Count;
            if (messages.Count > 0) partitionsWithMessages++;
        }

        Assert.That(totalMessages, Is.EqualTo(20), "All 20 messages should be received");
        Assert.That(partitionsWithMessages, Is.GreaterThan(1),
            "Messages should be distributed across multiple partitions");
    }

    [Test]
    public async Task Receiver_ConsumerGroup_MultipleConsumers_NoDuplicates()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:consumers",
            ConsumerGroup = "multi-consumer-group",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "consumers-test");
        var adapter = new RedisStreamAdapter("consumers-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "consumer-key");
        for (var i = 0; i < 10; i++)
            await adapter.QueueMessageBatchAsync(streamId, new[] { $"msg-{i}" }, null, null);

        var queueId = mapper.GetAllQueues().First();

        // Two receivers (simulate two silos) in the same consumer group
        var receiver1 = adapter.CreateReceiver(queueId);
        var receiver2 = adapter.CreateReceiver(queueId);
        await receiver1.Initialize(TimeSpan.FromSeconds(5));
        await receiver2.Initialize(TimeSpan.FromSeconds(5));

        var msgs1 = await receiver1.GetQueueMessagesAsync(100);
        var msgs2 = await receiver2.GetQueueMessagesAsync(100);

        // Consumer group guarantees: each message delivered to exactly one consumer
        Assert.That(msgs1.Count + msgs2.Count, Is.EqualTo(10),
            "Total messages across consumers should equal produced messages");
    }
}

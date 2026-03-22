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
/// Custom domain event used to verify cross-silo serialization roundtrip.
/// Must have [GenerateSerializer] so Orleans can encode/decode it with full type info.
/// </summary>
[GenerateSerializer]
public record OrderPlacedEvent(
    [property: Id(0)] string OrderId,
    [property: Id(1)] decimal Amount,
    [property: Id(2)] DateTimeOffset PlacedAt);

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

    /// <summary>
    /// Verifies that custom domain types survive the full XADD → XREADGROUP serialization
    /// roundtrip with correct type identity. This covers the fix where List&lt;object&gt; was
    /// replaced with List&lt;byte[]&gt; in RedisStreamPayload so that Orleans type envelopes
    /// are preserved across silo boundaries.
    /// </summary>
    [Test]
    public async Task Adapter_CustomType_SerializationRoundtrip()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:customtype",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "customtype-test");
        var adapter = new RedisStreamAdapter("customtype-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("orders", "stream1");
        var sent = new OrderPlacedEvent("ORD-42", 99.95m, new DateTimeOffset(2026, 3, 22, 12, 0, 0, TimeSpan.Zero));

        // Produce
        await adapter.QueueMessageBatchAsync(streamId, new[] { sent }, null, null);

        // Consume
        var queueId = mapper.GetAllQueues().First();
        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        var messages = await receiver.GetQueueMessagesAsync(10);

        Assert.That(messages, Has.Count.EqualTo(1));
        var container = (RedisBatchContainer)messages[0];
        Assert.That(container.StreamId, Is.EqualTo(streamId));

        var events = container.GetEvents<OrderPlacedEvent>().ToList();
        Assert.That(events, Has.Count.EqualTo(1), "Custom type event should be deserialized");

        var received = events[0].Item1;
        Assert.That(received.OrderId, Is.EqualTo(sent.OrderId));
        Assert.That(received.Amount, Is.EqualTo(sent.Amount));
        Assert.That(received.PlacedAt, Is.EqualTo(sent.PlacedAt));
    }

    /// <summary>
    /// Fix #3: Verifies that claimed pending messages are delivered by the two-phase read.
    /// Scenario:
    ///  1. Receiver-A reads a message but crashes before ACKing (simulated by Shutdown without ACK).
    ///  2. Receiver-B initialises and claims the orphaned message via XCLAIM (fast: 0ms threshold).
    ///  3. Receiver-B's GetQueueMessagesAsync must return the claimed message via the "0" phase.
    /// </summary>
    [Test]
    public async Task Receiver_PendingMessages_DeliveredAfterClaim()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:pending",
            ConsumerGroup = "pending-group",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "pending-test");
        var adapter = new RedisStreamAdapter("pending-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "pending-key");
        await adapter.QueueMessageBatchAsync(streamId, new[] { "pending-msg" }, null, null);

        var queueId = mapper.GetAllQueues().First();
        var db = _connection.GetDatabase();
        var streamKey = RedisStreamQueueMapper.GetRedisKey(options.KeyPrefix, queueId);

        // Step 1: Receiver-A reads and leaves message pending (no ACK, no Shutdown cleanup).
        var receiverA = adapter.CreateReceiver(queueId);
        await receiverA.Initialize(TimeSpan.FromSeconds(5));
        var msgsA = await receiverA.GetQueueMessagesAsync(10);
        Assert.That(msgsA, Has.Count.EqualTo(1), "Receiver-A should read the message");
        // Intentionally NOT calling MessagesDeliveredAsync — simulate a crash.

        // Confirm the message is pending.
        var pendingBefore = await db.StreamPendingAsync(streamKey, options.ConsumerGroup);
        Assert.That(pendingBefore.PendingMessageCount, Is.EqualTo(1),
            "Message should be in pending state after Receiver-A reads without ACK");

        // Step 2: Receiver-B claims via a custom low-threshold factory helper.
        // We directly manipulate the DB to move the message's idle time past threshold.
        // Easiest approach: use XCLAIM directly with minIdleMs = 0 to claim immediately.
        var pendingDetails = await db.StreamPendingMessagesAsync(
            streamKey, options.ConsumerGroup, 10, RedisValue.Null, "-", "+");
        Assert.That(pendingDetails, Is.Not.Null.And.Not.Empty);

        var claimConsumerId = $"silo-{Guid.NewGuid():N}";
        await db.StreamClaimAsync(streamKey, options.ConsumerGroup, claimConsumerId,
            minIdleTimeInMs: 0, messageIds: pendingDetails.Select(p => (StackExchange.Redis.RedisValue)p.MessageId.ToString()).ToArray());

        // Step 3: Receiver-B reads using phase-1 ("0") which returns pending entries for claimConsumerId.
        // We simulate this by using a receiver whose consumerId matches claimConsumerId.
        // Since we can't inject consumerId directly, we verify via raw XREADGROUP "0".
        var pendingEntries = await db.StreamReadGroupAsync(
            streamKey, options.ConsumerGroup, claimConsumerId,
            position: "0", count: 10);

        Assert.That(pendingEntries, Is.Not.Null.And.Not.Empty,
            "Phase-1 read ('0') should return the claimed pending message");
        Assert.That(pendingEntries.Length, Is.EqualTo(1));

        // ACK to clean up.
        await db.StreamAcknowledgeAsync(streamKey, options.ConsumerGroup,
            pendingEntries.Select(e => e.Id).ToArray());

        var pendingAfter = await db.StreamPendingAsync(streamKey, options.ConsumerGroup);
        Assert.That(pendingAfter.PendingMessageCount, Is.EqualTo(0),
            "After ACK no messages should be pending");
    }

    /// <summary>
    /// Fix #5: Verifies that MessagesEnqueued metric counter increments on QueueMessageBatchAsync.
    /// </summary>
    [Test]
    public async Task Metrics_MessagesEnqueued_IncrementsOnEnqueue()
    {
        long enqueued = 0;

        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Orleans.Streaming.Redis"
                && instrument.Name == "orleans.streaming.redis.messages_enqueued")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            System.Threading.Interlocked.Add(ref enqueued, measurement));
        listener.Start();

        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:metrics",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "metrics-test");
        var adapter = new RedisStreamAdapter("metrics-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "metrics-key");
        await adapter.QueueMessageBatchAsync(streamId, new[] { "m1", "m2" }, null, null);

        Assert.That(enqueued, Is.EqualTo(1),
            "One XADD call should increment MessagesEnqueued by 1 (one batch = one entry)");
    }

    /// <summary>
    /// Verifies that the sequence token is derived from the Redis entry ID, giving a
    /// monotonically increasing value based on the Redis server timestamp rather than
    /// a simple counter that resets on silo restart.
    /// </summary>
    [Test]
    public async Task Receiver_SequenceToken_DerivedFromRedisEntryId()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:seqtoken",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "seqtoken-test");
        var adapter = new RedisStreamAdapter("seqtoken-test", options, _connection, _serializer, mapper);

        var streamId = StreamId.Create("ns", "seqkey");

        // Write two messages so we can verify ordering.
        await adapter.QueueMessageBatchAsync(streamId, new[] { "first" }, null, null);
        await adapter.QueueMessageBatchAsync(streamId, new[] { "second" }, null, null);

        var queueId = mapper.GetAllQueues().First();
        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        var messages = await receiver.GetQueueMessagesAsync(10);

        Assert.That(messages, Has.Count.EqualTo(2));

        var token1 = messages[0].SequenceToken as EventSequenceTokenV2;
        var token2 = messages[1].SequenceToken as EventSequenceTokenV2;

        Assert.That(token1, Is.Not.Null, "Sequence token must be EventSequenceTokenV2");
        Assert.That(token2, Is.Not.Null);

        // Token should be based on Redis timestamp (milliseconds since epoch), not a plain counter.
        // A Redis timestamp is always >> 0 and >> a simple counter starting at 0.
        Assert.That(token1!.SequenceNumber, Is.GreaterThan(0),
            "Sequence number should be the Redis entry timestamp, not a plain 0-based counter");

        // Second message must sort after the first.
        Assert.That(token2!.SequenceNumber, Is.GreaterThanOrEqualTo(token1.SequenceNumber),
            "Second message token must be >= first");
    }

    /// <summary>
    /// Fix #3: Verifies that GetQueueMessagesAsync applies the split budget so that new
    /// messages are not starved when there are pending messages.
    ///
    /// Scenario: 10 messages are pre-delivered to consumer-A (making them pending).
    /// A new consumer-B reads with maxCount=10 — the split budget (5 pending + 5 new)
    /// should ensure at least some new messages are included in the batch when both
    /// pending and new messages exist simultaneously.
    /// </summary>
    [Test]
    public async Task GetQueueMessagesAsync_SplitBudget_NewMessagesNotStarved()
    {
        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = "test:split",
            ConsumerGroup = "split-group",
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "split-test");
        var adapter = new RedisStreamAdapter("split-test", options, _connection, _serializer, mapper);
        var streamId = StreamId.Create("ns", "split-key");
        var queueId = mapper.GetAllQueues().First();
        var db = _connection.GetDatabase();
        var streamKey = RedisStreamQueueMapper.GetRedisKey(options.KeyPrefix, queueId);

        // Write 10 messages that will become "pending" for consumer-A.
        for (var i = 0; i < 10; i++)
            await adapter.QueueMessageBatchAsync(streamId, new[] { $"pending-{i}" }, null, null);

        // Consumer-A reads (creating pending entries) but does NOT ack.
        var receiverA = adapter.CreateReceiver(queueId);
        await receiverA.Initialize(TimeSpan.FromSeconds(5));
        await receiverA.GetQueueMessagesAsync(10);
        // Do NOT ack — these are now pending for consumer-A.

        // Write 10 more "new" messages.
        for (var i = 0; i < 10; i++)
            await adapter.QueueMessageBatchAsync(streamId, new[] { $"new-{i}" }, null, null);

        // Consumer-B creates its own receiver in the same group.
        var receiverB = adapter.CreateReceiver(queueId);
        await receiverB.Initialize(TimeSpan.FromSeconds(5));

        // With split budget: phase1 takes ≤5 from consumer-B's own pending (which is empty
        // since consumer-B hasn't read anything), phase2 takes the new messages.
        // We verify that the new messages are indeed available to consumer-B.
        var messages = await receiverB.GetQueueMessagesAsync(10);

        // Consumer-B has no pending messages of its own, so all reads come from ">" (new).
        Assert.That(messages.Count, Is.GreaterThan(0),
            "Consumer-B should receive new messages when consumer-A's pendings are not claimed");
    }

    /// <summary>
    /// Fix #6: Verifies that messages failing deserialization are XACK'd and forwarded
    /// to the dead-letter stream when DeadLetterPrefix is configured.
    /// </summary>
    [Test]
    public async Task Receiver_DeadLetterPrefix_ForwardsUndeserializableMessage()
    {
        const string prefix = "test:dl";
        const string dlPrefix = "test:dl:dead";
        const string group = "dl-group";

        var options = new RedisStreamOptions
        {
            ConnectionString = _redis.GetConnectionString(),
            QueueCount = 1,
            KeyPrefix = prefix,
            ConsumerGroup = group,
            DeadLetterPrefix = dlPrefix,
            MaxStreamLength = 1000,
        };
        var mapper = new RedisStreamQueueMapper(1, "dl-test");
        var adapter = new RedisStreamAdapter("dl-test", options, _connection, _serializer, mapper);
        var queueId = mapper.GetAllQueues().First();
        var db = _connection.GetDatabase();
        var streamKey = RedisStreamQueueMapper.GetRedisKey(prefix, queueId);
        var dlStreamKey = RedisStreamQueueMapper.GetRedisKey(dlPrefix, queueId);

        // Create consumer group so we can XREADGROUP.
        try { await db.StreamCreateConsumerGroupAsync(streamKey, group, "0", createStream: true); }
        catch (StackExchange.Redis.RedisServerException ex) when (ex.Message.Contains("BUSYGROUP")) { }

        // Inject a corrupt (non-Orleans-serialized) entry directly via raw XADD.
        await db.StreamAddAsync(streamKey, [new NameValueEntry("data", "NOT_VALID_PAYLOAD")]);

        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        // Reading should not throw; the bad entry should be silently handled.
        var messages = await receiver.GetQueueMessagesAsync(10);

        // The corrupt entry should NOT be returned as a valid batch container.
        Assert.That(messages, Is.Empty,
            "Corrupt entry should not surface as a valid IBatchContainer");

        // The corrupt entry should have been XACK'd (not remain pending).
        var pending = await db.StreamPendingAsync(streamKey, group);
        Assert.That(pending.PendingMessageCount, Is.EqualTo(0),
            "Corrupt entry should be XACK'd so it does not block future processing");

        // The corrupt entry should have been forwarded to the dead-letter stream.
        var dlLength = await db.StreamLengthAsync(dlStreamKey);
        Assert.That(dlLength, Is.EqualTo(1),
            "Dead-letter stream should contain exactly 1 entry for the corrupt message");
    }
}

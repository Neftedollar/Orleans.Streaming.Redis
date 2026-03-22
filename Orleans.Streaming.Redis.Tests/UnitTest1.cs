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
    public void GetRedisKey_FormatsCorrectly()
    {
        var mapper = new RedisStreamQueueMapper(4, "provider");
        var queue = mapper.GetAllQueues().First();
        var key = RedisStreamQueueMapper.GetRedisKey("orleans:stream", queue);

        Assert.That(key, Does.StartWith("orleans:stream:"));
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
}

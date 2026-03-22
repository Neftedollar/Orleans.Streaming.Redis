using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.Redis.Configuration;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// Factory that creates RedisStreamAdapter instances.
/// Manages the Redis connection lifecycle and queue mapper.
/// </summary>
public class RedisStreamAdapterFactory : IQueueAdapterFactory
{
    private readonly string _providerName;
    private readonly RedisStreamOptions _options;
    private readonly Serializer _serializer;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly RedisStreamQueueMapper _queueMapper;
    private IConnectionMultiplexer? _redis;

    public RedisStreamAdapterFactory(
        string providerName,
        RedisStreamOptions options,
        Serializer serializer,
        ILoggerFactory? loggerFactory = null)
    {
        _providerName = providerName;
        _options = options;
        _serializer = serializer;
        _loggerFactory = loggerFactory;
        _queueMapper = new RedisStreamQueueMapper(options.QueueCount, providerName);
    }

    public async Task<IQueueAdapter> CreateAdapter()
    {
        _redis = await ConnectionMultiplexer.ConnectAsync(_options.ConnectionString);

        return new RedisStreamAdapter(
            _providerName,
            _options,
            _redis,
            _serializer,
            _queueMapper,
            _loggerFactory);
    }

    public IQueueAdapterCache GetQueueAdapterCache()
        => new SimpleQueueAdapterCache(_providerName);

    public IStreamQueueMapper GetStreamQueueMapper()
        => _queueMapper;

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());

    /// <summary>
    /// Creates the factory from DI. Used as the delegate for AddPersistentStreams.
    /// </summary>
    public static RedisStreamAdapterFactory Create(IServiceProvider services, string providerName)
    {
        var options = services.GetOptionsByName<RedisStreamOptions>(providerName);
        var serializer = services.GetRequiredService<Serializer>();
        var loggerFactory = services.GetService<ILoggerFactory>();
        return new RedisStreamAdapterFactory(providerName, options, serializer, loggerFactory);
    }
}

/// <summary>
/// Minimal queue adapter cache wrapping <see cref="RedisQueueCache"/>.
/// </summary>
internal class SimpleQueueAdapterCache : IQueueAdapterCache
{
    private readonly string _providerName;

    public SimpleQueueAdapterCache(string providerName)
    {
        _providerName = providerName;
    }

    public IQueueCache CreateQueueCache(QueueId queueId)
        => new RedisQueueCache(4096);
}

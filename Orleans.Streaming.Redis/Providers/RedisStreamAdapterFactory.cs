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
/// Implements <see cref="IAsyncDisposable"/> for clean shutdown.
/// </summary>
public class RedisStreamAdapterFactory : IQueueAdapterFactory, IAsyncDisposable
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
        // Fix #6: validate options before attempting to connect.
        _options.Validate();

        // Fix #2: configure ConnectionMultiplexer with reconnect settings.
        var configOptions = ConfigurationOptions.Parse(_options.ConnectionString);
        configOptions.AbortOnConnectFail = false;
        configOptions.ConnectRetry = 3;
        configOptions.ReconnectRetryPolicy = new ExponentialRetry(100);

        _redis = await ConnectionMultiplexer.ConnectAsync(configOptions);

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
    /// Fix #8: dispose the ConnectionMultiplexer on shutdown.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_redis is not null)
        {
            await _redis.CloseAsync();
            _redis.Dispose();
            _redis = null;
        }
    }

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

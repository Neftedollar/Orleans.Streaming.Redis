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

    /// <summary>
    /// Initialises the factory.
    /// </summary>
    /// <param name="providerName">Stream provider name.</param>
    /// <param name="options">Redis stream configuration.</param>
    /// <param name="serializer">Orleans serializer.</param>
    /// <param name="loggerFactory">Optional logger factory.</param>
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public IQueueAdapterCache GetQueueAdapterCache()
        => new SimpleQueueAdapterCache(_providerName, _options.CacheSize);

    /// <inheritdoc />
    public IStreamQueueMapper GetStreamQueueMapper()
        => _queueMapper;

    /// <inheritdoc />
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        var logger = _loggerFactory?.CreateLogger<LoggingStreamDeliveryFailureHandler>()
                     ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<LoggingStreamDeliveryFailureHandler>.Instance;
        return Task.FromResult<IStreamFailureHandler>(new LoggingStreamDeliveryFailureHandler(logger));
    }

    /// <summary>
    /// Disposes the ConnectionMultiplexer on shutdown.
    /// NOTE: CloseAsync + Dispose is the recommended SE.Redis teardown sequence.
    /// We swallow ObjectDisposedException here because Orleans may dispose this factory
    /// after the host has already stopped (e.g. in WebApplicationFactory teardown in tests),
    /// at which point SE.Redis internals may already be gone. Logging the warning is enough.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        var redis = Interlocked.Exchange(ref _redis, null);
        if (redis is null)
            return;

        try
        {
            await redis.CloseAsync();
        }
        catch (ObjectDisposedException) { /* already closed, nothing to do */ }
        catch (Exception ex)
        {
            _loggerFactory?.CreateLogger<RedisStreamAdapterFactory>()
                .LogWarning(ex, "RedisStreamAdapterFactory: CloseAsync failed during dispose.");
        }
        finally
        {
            try { redis.Dispose(); }
            catch (ObjectDisposedException) { /* already disposed */ }
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
/// Cache size is configurable via <see cref="RedisStreamOptions.CacheSize"/>.
/// </summary>
internal class SimpleQueueAdapterCache : IQueueAdapterCache
{
    private readonly string _providerName;
    private readonly int _cacheSize;

    /// <summary>
    /// Initialises the cache factory.
    /// </summary>
    /// <param name="providerName">Orleans stream provider name.</param>
    /// <param name="cacheSize">Maximum number of batch containers to hold per partition cache.</param>
    public SimpleQueueAdapterCache(string providerName, int cacheSize = 4096)
    {
        _providerName = providerName;
        _cacheSize = cacheSize;
    }

    /// <summary>Creates a new per-queue cache for the specified queue partition.</summary>
    public IQueueCache CreateQueueCache(QueueId queueId)
        => new RedisQueueCache(_cacheSize);
}

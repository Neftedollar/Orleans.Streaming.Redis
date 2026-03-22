using Microsoft.Extensions.Hosting;
using Orleans.Streaming.Redis.Providers;

namespace Orleans.Streaming.Redis.Configuration;

/// <summary>
/// Tracks all <see cref="RedisStreamAdapterFactory"/> instances created during silo startup.
/// Used by <see cref="RedisStreamFactoryLifetimeService"/> to dispose them on host shutdown.
/// </summary>
internal sealed class RedisStreamFactoryRegistry
{
    private readonly List<RedisStreamAdapterFactory> _factories = [];

    /// <summary>Registers a factory for lifecycle management.</summary>
    public void Register(string providerName, RedisStreamAdapterFactory factory)
    {
        lock (_factories)
            _factories.Add(factory);
    }

    /// <summary>Returns a snapshot of all registered factories.</summary>
    public IReadOnlyList<RedisStreamAdapterFactory> GetAll()
    {
        lock (_factories)
            return [.. _factories];
    }
}

/// <summary>
/// <see cref="IHostedService"/> that disposes all registered
/// <see cref="RedisStreamAdapterFactory"/> instances when the host stops.
/// This ensures that the underlying <see cref="StackExchange.Redis.IConnectionMultiplexer"/>
/// is gracefully closed even though Orleans does not call
/// <see cref="IAsyncDisposable.DisposeAsync"/> on <c>IQueueAdapterFactory</c> objects.
/// </summary>
internal sealed class RedisStreamFactoryLifetimeService : IHostedService
{
    private readonly RedisStreamFactoryRegistry _registry;

    /// <summary>Initialises the service with the factory registry.</summary>
    public RedisStreamFactoryLifetimeService(RedisStreamFactoryRegistry registry)
    {
        _registry = registry;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var factory in _registry.GetAll())
            await factory.DisposeAsync();
    }
}

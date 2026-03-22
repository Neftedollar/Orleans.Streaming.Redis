using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Streaming.Redis.Providers;

namespace Orleans.Streaming.Redis.Configuration;

/// <summary>
/// Extension methods for registering the Redis Streams persistent stream provider.
/// </summary>
public static class SiloBuilderExtensions
{
    /// <summary>
    /// Adds a Redis Streams persistent stream provider to the silo.
    /// The factory's <see cref="IAsyncDisposable"/> lifecycle is tied to the host via
    /// a registered <see cref="IHostedService"/> so that the Redis connection is
    /// gracefully closed on silo shutdown.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">Provider name (used in stream subscriptions).</param>
    /// <param name="configure">Action to configure Redis stream options.</param>
    /// <param name="configurePulling">
    /// Optional callback for configuring the persistent stream pulling agent,
    /// e.g., poll interval, init timeout, queue balancing strategy.
    /// When <see langword="null"/>, platform defaults are used.
    /// </param>
    public static ISiloBuilder AddRedisStreams(
        this ISiloBuilder builder,
        string name,
        Action<RedisStreamOptions> configure,
        Action<ISiloPersistentStreamConfigurator>? configurePulling = null)
    {
        builder.Services.AddOptions<RedisStreamOptions>(name).Configure(configure);

        builder.AddPersistentStreams(
            name,
            (sp, providerName) =>
            {
                var factory = RedisStreamAdapterFactory.Create(sp, providerName);
                // Register the factory instance so the hosted-service wrapper can dispose it.
                sp.GetRequiredService<RedisStreamFactoryRegistry>().Register(providerName, factory);
                return factory;
            },
            configurator =>
            {
                configurePulling?.Invoke(configurator);
            });

        // Register the registry and the hosted service exactly once (idempotent).
        builder.Services.AddSingleton<RedisStreamFactoryRegistry>();
        builder.Services.AddHostedService<RedisStreamFactoryLifetimeService>();

        return builder;
    }
}

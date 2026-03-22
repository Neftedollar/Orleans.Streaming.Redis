using Microsoft.Extensions.DependencyInjection;
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
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">Provider name (used in stream subscriptions).</param>
    /// <param name="configure">Action to configure Redis stream options.</param>
    public static ISiloBuilder AddRedisStreams(
        this ISiloBuilder builder,
        string name,
        Action<RedisStreamOptions> configure)
    {
        builder.Services.AddOptions<RedisStreamOptions>(name).Configure(configure);

        builder.AddPersistentStreams(
            name,
            RedisStreamAdapterFactory.Create,
            configurator => { });

        return builder;
    }
}

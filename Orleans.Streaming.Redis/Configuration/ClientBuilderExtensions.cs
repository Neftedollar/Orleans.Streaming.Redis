using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Streaming.Redis.Providers;

namespace Orleans.Streaming.Redis.Configuration;

/// <summary>
/// Extension methods for registering the Redis Streams stream provider on an
/// external Orleans client (<see cref="IClientBuilder"/>).
/// External clients use the provider to produce messages; they do not pull or consume.
/// </summary>
public static class ClientBuilderExtensions
{
    /// <summary>
    /// Adds a Redis Streams stream provider to an external Orleans client.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">Provider name — must match the name used on the silo.</param>
    /// <param name="configure">Action to configure Redis stream options.</param>
    /// <param name="configurePersistentStreams">
    /// Optional callback to configure the client-side persistent stream provider,
    /// e.g., stream lifecycle options.
    /// When <see langword="null"/>, platform defaults are used.
    /// </param>
    public static IClientBuilder AddRedisStreams(
        this IClientBuilder builder,
        string name,
        Action<RedisStreamOptions> configure,
        Action<IClusterClientPersistentStreamConfigurator>? configurePersistentStreams = null)
    {
        builder.Services.AddOptions<RedisStreamOptions>(name).Configure(configure);

        builder.AddPersistentStreams(
            name,
            RedisStreamAdapterFactory.Create,
            configurator =>
            {
                configurePersistentStreams?.Invoke(configurator);
            });

        return builder;
    }
}

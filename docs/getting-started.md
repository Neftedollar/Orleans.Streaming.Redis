# Getting Started

## Prerequisites

- .NET 8.0, 9.0, or 10.0
- Redis 5.0+ (Streams support required)
- An Orleans 10.x application

## Installation

```bash
dotnet add package Orleans.Streaming.Redis
```

## Minimal Setup

### 1. Register the provider in your silo

```csharp
using Orleans.Streaming.Redis.Configuration;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();

    // Persistent stream provider backed by Redis Streams.
    silo.AddRedisStreams("StreamProvider", options =>
    {
        options.ConnectionString = "localhost:6379";
    });

    // PubSubStore is required for stream subscriptions.
    // Use Redis in production so subscriptions survive silo restarts.
    silo.AddMemoryGrainStorage("PubSubStore");
});
```

### 2. Produce events from a grain

```csharp
public class ProducerGrain : Grain, IProducerGrain
{
    public async Task PublishOrderAsync(string orderId, decimal amount)
    {
        var provider = this.GetStreamProvider("StreamProvider");
        var stream = provider.GetStream<OrderEvent>("orders", orderId);

        await stream.OnNextAsync(new OrderEvent(orderId, amount, DateTimeOffset.UtcNow));
    }
}

[GenerateSerializer]
public record OrderEvent(
    [property: Id(0)] string OrderId,
    [property: Id(1)] decimal Amount,
    [property: Id(2)] DateTimeOffset Timestamp);
```

### 3. Consume events in a subscriber grain

```csharp
[ImplicitStreamSubscription("orders")]
public class OrderProcessorGrain : Grain, IOrderProcessorGrain, IAsyncObserver<OrderEvent>
{
    public override async Task OnActivateAsync(CancellationToken ct)
    {
        var provider = this.GetStreamProvider("StreamProvider");
        var stream = provider.GetStream<OrderEvent>("orders", this.GetPrimaryKeyString());
        await stream.SubscribeAsync(this);
    }

    public Task OnNextAsync(OrderEvent item, StreamSequenceToken? token = null)
    {
        // Process the order event.
        Console.WriteLine($"Processing order {item.OrderId}: ${item.Amount}");
        return Task.CompletedTask;
    }

    public Task OnCompletedAsync() => Task.CompletedTask;
    public Task OnErrorAsync(Exception ex) => Task.CompletedTask;
}
```

### 4. Run

```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:7

# Run your app
dotnet run
```

## What happens under the hood

1. `ProducerGrain` calls `stream.OnNextAsync(event)`
2. `RedisStreamAdapter` serializes the event and calls `XADD` to a Redis Stream
3. Orleans PullingAgent periodically calls `XREADGROUP` to fetch new messages
4. The message is deserialized and delivered to `OrderProcessorGrain.OnNextAsync`
5. After delivery, `XACK` acknowledges the message in the consumer group

## Next steps

- [Configuration Reference](configuration.md) — all available options
- [Architecture](architecture.md) — how the provider works internally
- [Production Deployment](production.md) — Redis requirements, monitoring, scaling
- [Troubleshooting](troubleshooting.md) — common issues and solutions

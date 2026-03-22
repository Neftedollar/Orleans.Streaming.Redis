using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.Redis.Providers;

/// <summary>
/// <see cref="IStreamFailureHandler"/> that logs delivery and subscription failures at
/// <see cref="LogLevel.Error"/> level instead of silently ignoring them.
/// </summary>
internal sealed class LoggingStreamDeliveryFailureHandler : IStreamFailureHandler
{
    private readonly ILogger _logger;

    /// <summary>
    /// Initialises the handler with the supplied logger.
    /// </summary>
    /// <param name="logger">Logger used to emit error messages on failures.</param>
    /// <param name="shouldFaultSubscriptionOnError">
    /// When <see langword="true"/>, the subscription is faulted after a delivery failure.
    /// Default is <see langword="false"/>.
    /// </param>
    public LoggingStreamDeliveryFailureHandler(ILogger logger, bool shouldFaultSubscriptionOnError = false)
    {
        _logger = logger;
        ShouldFaultSubsriptionOnError = shouldFaultSubscriptionOnError;
    }

    /// <inheritdoc />
    public bool ShouldFaultSubsriptionOnError { get; }

    /// <inheritdoc />
    public Task OnDeliveryFailure(
        GuidId subscriptionId,
        string streamProviderName,
        StreamId streamId,
        StreamSequenceToken sequenceToken)
    {
        _logger.LogError(
            "Stream delivery failure: provider={StreamProvider}, stream={StreamId}, " +
            "subscription={SubscriptionId}, token={SequenceToken}",
            streamProviderName, streamId, subscriptionId, sequenceToken);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnSubscriptionFailure(
        GuidId subscriptionId,
        string streamProviderName,
        StreamId streamId,
        StreamSequenceToken sequenceToken)
    {
        _logger.LogError(
            "Stream subscription failure: provider={StreamProvider}, stream={StreamId}, " +
            "subscription={SubscriptionId}, token={SequenceToken}",
            streamProviderName, streamId, subscriptionId, sequenceToken);

        return Task.CompletedTask;
    }
}

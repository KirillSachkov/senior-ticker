using Polly;
using Polly.Retry;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Политика переподключения: бесконечный retry с экспоненциальным backoff и jitter. Дефолтный
/// ShouldHandle в Polly v8 ретраит любое исключение, кроме OperationCanceledException. Значит отмена
/// (штатная остановка) распространяется немедленно, а любой сетевой сбой или закрытие ведут к
/// переподключению. onRetry нужен для логирования.
/// </summary>
public static class ResiliencePipelineFactory
{
    public static ResiliencePipeline CreateReconnectPipeline(
        WebSocketConnectorOptions options, Action<Exception, TimeSpan, int> onRetry)
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = int.MaxValue,
                BackoffType = DelayBackoffType.Exponential,
                Delay = options.ReconnectBaseDelay,
                MaxDelay = options.ReconnectMaxDelay,
                UseJitter = true,
                OnRetry = args =>
                {
                    onRetry(args.Outcome.Exception!, args.RetryDelay, args.AttemptNumber);
                    return default;
                },
            })
            .Build();
    }
}

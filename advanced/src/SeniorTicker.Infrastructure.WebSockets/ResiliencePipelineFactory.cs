using Polly;
using Polly.Retry;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Reconnect-политика: бесконечный retry с экспоненциальным backoff + jitter. Дефолтный
/// ShouldHandle Polly v8 ретраит любое исключение КРОМЕ OperationCanceledException — то есть
/// отмена (graceful shutdown) распространяется немедленно (фикс #6), а любой сетевой сбой/закрытие
/// → переподключение. onRetry — для логирования.
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

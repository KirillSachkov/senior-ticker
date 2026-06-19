using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Observability;

/// <summary>
/// Публикует снимок метрик раз в <see cref="MetricsConfig.IntervalSeconds"/> (§12): дельты
/// in/dedup/written/dropped, <b>gap = in − written</b> (успевают ли writers) и глубину каналов
/// (какой лейн лимитирует). <see cref="PeriodicTimer"/> на <see cref="TimeProvider"/> — детерминирован
/// в тестах. Регистрируется до конвейера → по LIFO останавливается ПОСЛЕ его дренажа, поэтому видим
/// метрики и во время дренажа.
/// </summary>
public sealed class MetricsBackgroundService(
    MetricsSink metrics,
    ITickPipeline pipeline,
    MetricsConfig config,
    TimeProvider time,
    ILogger<MetricsBackgroundService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, config.IntervalSeconds));
        var previous = metrics.Snapshot();
        using var timer = new PeriodicTimer(interval, time);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                var current = metrics.Snapshot();
                logger.LogInformation(
                    "metrics in={In} (+{InDelta}) deduped={Deduped} (+{DedupedDelta}) written={Written} (+{WrittenDelta}) dropped={Dropped} (+{DroppedDelta}) gap={Gap} ingestDepth={IngestDepth} batchDepth={BatchDepth}",
                    current.Received, current.Received - previous.Received,
                    current.Deduplicated, current.Deduplicated - previous.Deduplicated,
                    current.Written, current.Written - previous.Written,
                    current.Dropped, current.Dropped - previous.Dropped,
                    current.Gap,
                    pipeline.IngestDepth,
                    pipeline.BatchDepth);
                previous = current;
            }
        }
        catch (OperationCanceledException)
        {
            // штатная остановка
        }
    }
}

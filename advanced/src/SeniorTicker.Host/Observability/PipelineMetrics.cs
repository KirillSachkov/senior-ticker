using Microsoft.Extensions.Hosting;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Observability;

/// <summary>
/// Наблюдаемые gauge'ы глубины каналов на Meter сервиса (§12): сколько тиков ждёт во входном канале
/// и сколько батчей в канале записи — какой лейн лимитирует под нагрузкой. Gauge = мгновенное значение,
/// MeterProvider опрашивает его на каждом экспорте. Хостед-сервис, чтобы инструменты создались на старте
/// и жили до конца остановки: зарегистрирован до конвейера → по LIFO останавливается ПОСЛЕ того, как он дочитал остаток,
/// поэтому глубины видны и во время дочитывания остатка (§5.4).
/// </summary>
public sealed class PipelineMetrics(MetricsSink metrics, TickPipeline pipeline) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        metrics.Meter.CreateObservableGauge("ticker.ingest.queue.depth", () => pipeline.IngestDepth,
            "{tick}", "Тики, ожидающие во входном канале.");
        metrics.Meter.CreateObservableGauge("ticker.batch.queue.depth", () => pipeline.BatchDepth,
            "{batch}", "Батчи, ожидающие в канале записи.");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

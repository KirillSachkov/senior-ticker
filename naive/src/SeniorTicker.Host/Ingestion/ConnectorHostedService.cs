using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SeniorTicker.Application;
using SeniorTicker.Host.Configuration;

namespace SeniorTicker.Host.Ingestion;

/// <summary>
/// Запускает все enabled-коннекторы и держит их живыми до остановки. Каждый коннектор обрабатывает
/// тик на своём потоке через <c>ITickIngestor</c> (наивный вариант: общий дедуп + запись по тику).
/// На остановке <c>stoppingToken</c> гасит коннекторы; дренажа нет, незаписанные тики теряются.
/// Очереди и двухфазного завершения здесь нет — это есть в advanced-решении.
/// </summary>
public sealed class ConnectorHostedService(
    ConnectorFactory factory,
    IOptions<SeniorTickerOptions> options,
    ILogger<ConnectorHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connectors = factory.CreateEnabled(options.Value.Exchanges);
        if (connectors.Count == 0)
        {
            logger.LogWarning("No enabled exchange connectors configured — nothing to ingest.");
            return;
        }

        logger.LogInformation("Starting {Count} connector(s): {Names}",
            connectors.Count, string.Join(", ", connectors.Select(c => c.Name)));

        try
        {
            await Task.WhenAll(connectors.Select(c => c.RunAsync(stoppingToken)));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // штатная остановка — коннекторы погашены отменой
        }
    }
}

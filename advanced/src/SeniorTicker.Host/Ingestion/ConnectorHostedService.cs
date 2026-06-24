using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SeniorTicker.Application;
using SeniorTicker.Host.Configuration;

namespace SeniorTicker.Host.Ingestion;

/// <summary>
/// Запускает все включённые коннекторы и держит их живыми до остановки. Регистрируется последним,
/// поэтому по LIFO-порядку Generic Host останавливается первым: отмена <c>stoppingToken</c> гасит
/// переподключение (Polly v8 не ретраит отмену), коннекторы выходят, вход конвейера закрыт, и только
/// потом начинается дочитывание остатка (<see cref="Pipeline.PipelineHostedService"/>). Это первая
/// фаза двухфазной остановки.
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
            logger.LogWarning("No enabled exchange connectors configured, nothing to ingest.");
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
            // штатная остановка: коннекторы погашены отменой
        }
    }
}

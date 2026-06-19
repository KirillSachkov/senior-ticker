using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SeniorTicker.Application;
using SeniorTicker.Host.Configuration;

namespace SeniorTicker.Host.Ingestion;

/// <summary>
/// Запускает все enabled-коннекторы и держит их живыми до остановки. Регистрируется ПОСЛЕДНИМ →
/// по LIFO-порядку Generic Host останавливается ПЕРВЫМ: отмена <c>stoppingToken</c> гасит reconnect
/// (Polly v8 не ретраит OCE, #6), коннекторы выходят, вход конвейера перекрыт — и только потом
/// начинается дренаж (<see cref="Pipeline.PipelineHostedService"/>). Это фаза ① двухфазного шатдауна §5.4.
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

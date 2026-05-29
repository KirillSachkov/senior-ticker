using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using Polly;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Базовый коннектор: connect → receive (растущий буфер) → parse → validate → ingest, с
/// автопереподключением через Polly. Чисто async (ни одного заблокированного потока). Логирует
/// connect/disconnect/error. Конкретный коннектор задаёт лишь IMessageParser (формат биржи).
/// </summary>
public sealed class WebSocketConnectorBase(
    WebSocketConnectorOptions options,
    IMessageParser parser,
    ITickIngestor ingestor,
    IMetricsSink metrics,
    TimeProvider time,
    ILogger logger,
    Func<ClientWebSocket> socketFactory) : IExchangeConnector
{
    public string Name => options.Name;

    public async Task RunAsync(CancellationToken ct)
    {
        var pipeline = ResiliencePipelineFactory.CreateReconnectPipeline(options, (ex, delay, attempt) =>
            logger.LogWarning(ex, "{Name}: reconnect attempt {Attempt} in {Delay}", Name, attempt + 1, delay));
        try
        {
            await pipeline.ExecuteAsync(async token => await ConnectAndConsumeAsync(token).ConfigureAwait(false), ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("{Name}: stopped (shutdown)", Name);
        }
    }

    private async Task ConnectAndConsumeAsync(CancellationToken ct)
    {
        using var socket = socketFactory();
        await socket.ConnectAsync(options.Url, ct).ConfigureAwait(false);
        logger.LogInformation("{Name}: connected to {Url}", Name, options.Url);

        var receiver = new WebSocketMessageReceiver(options.InitialReceiveBufferBytes, options.MaxMessageBytes);
        while (!ct.IsCancellationRequested)
        {
            using var msg = await receiver.ReceiveAsync(socket, ct).ConfigureAwait(false);
            if (msg.IsClosed)
            {
                logger.LogInformation("{Name}: server closed connection — reconnecting", Name);
                throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely); // → Polly reconnect
            }

            if (!parser.TryParse(msg.Span, time.GetUtcNow(), out var tick)) { metrics.OnDropped(); continue; }
            if (!TickValidator.IsValid(tick, time)) { metrics.OnDropped(); continue; }
            await ingestor.IngestAsync(tick, ct).ConfigureAwait(false);
        }
    }
}

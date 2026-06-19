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
        await ConnectWithTimeoutAsync(socket, ct).ConfigureAwait(false);
        logger.LogInformation("{Name}: connected to {Url}", Name, options.Url);

        var receiver = new WebSocketMessageReceiver(options.InitialReceiveBufferBytes, options.MaxMessageBytes);
        while (!ct.IsCancellationRequested)
        {
            using var msg = await ReceiveWithIdleTimeoutAsync(receiver, socket, ct).ConfigureAwait(false);
            if (msg.IsClosed)
            {
                logger.LogInformation("{Name}: server closed connection — reconnecting", Name);
                throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely); // → Polly reconnect
            }

            // No-throw гейт уровня конвейера: НИ ОДИН парсер не должен уронить receive-loop недоверенным
            // кадром (иначе тривиальный DoS под int.MaxValue reconnect). Дополняет внутренние catch парсеров —
            // даже будущий парсер с неполным catch-списком тут безопасно деградирует в drop+метрику.
            bool parsed;
            Tick tick = default;
            try
            {
                parsed = parser.TryParse(msg.Span, time.GetUtcNow(), out tick);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                metrics.OnDropped();
                logger.LogWarning(ex, "{Name}: parser threw on a frame — dropped", Name);
                continue;
            }

            if (!parsed) { metrics.OnDropped(); continue; }
            if (!TickValidator.IsValid(tick, time)) { metrics.OnDropped(); continue; }
            await ingestor.IngestAsync(tick, ct).ConfigureAwait(false);
        }
    }

    /// <summary>ConnectAsync с дедлайном (WS-2). Таймаут → TimeoutException (не shutdown-OCE) → Polly reconnect.</summary>
    private async Task ConnectWithTimeoutAsync(ClientWebSocket socket, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(options.ConnectTimeout);
        try
        {
            await socket.ConnectAsync(options.Url, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new TimeoutException($"{Name}: WS connect to {options.Url} exceeded {options.ConnectTimeout}");
        }
    }

    /// <summary>ReceiveAsync с idle-дедлайном (WS-1, анти-slowloris). Тишина дольше idle → reconnect.</summary>
    private async ValueTask<ReceivedMessage> ReceiveWithIdleTimeoutAsync(
        WebSocketMessageReceiver receiver, ClientWebSocket socket, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(options.ReceiveIdleTimeout);
        try
        {
            return await receiver.ReceiveAsync(socket, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new TimeoutException($"{Name}: no WS data within idle timeout {options.ReceiveIdleTimeout}");
        }
    }
}

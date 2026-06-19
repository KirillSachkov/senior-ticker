using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using SeniorTicker.Application;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Наивный коннектор: connect → один ReceiveAsync в фиксированный буфер → parse → ingest, reconnect
/// простым циклом с паузой. Слабее advanced: буфер фиксированный (длинное сообщение усекается), кадр
/// читается без сборки до EndOfMessage, нет таймаутов connect/idle (молчащий peer завесит цикл), нет
/// валидации тика, reconnect повторяет на любую ошибку. Парсер задаёт формат биржи.
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
    private const int BufferBytes = 16 * 1024; // фиксированный буфер; длинное сообщение не вместится

    public string Name => options.Name;

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await ConnectAndConsumeAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return; // штатная остановка
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "{Name}: соединение оборвалось, переподключение через 1с", Name);
                try { await Task.Delay(1000, ct); } catch (OperationCanceledException) { return; }
            }
        }
    }

    private async Task ConnectAndConsumeAsync(CancellationToken ct)
    {
        using var socket = socketFactory();
        await socket.ConnectAsync(options.Url, ct); // без таймаута: зависший connect ждёт бесконечно
        logger.LogInformation("{Name}: connected to {Url}", Name, options.Url);

        var buffer = new byte[BufferBytes];
        while (!ct.IsCancellationRequested)
        {
            var result = await socket.ReceiveAsync(buffer.AsMemory(), ct); // без idle-таймаута
            if (result.MessageType == WebSocketMessageType.Close)
            {
                logger.LogInformation("{Name}: server closed connection", Name);
                return; // наружу — reconnect
            }

            // один кадр как есть: фрагмент или сообщение длиннее буфера усекается
            var span = buffer.AsSpan(0, result.Count);
            if (!parser.TryParse(span, time.GetUtcNow(), out var tick))
            {
                metrics.OnDropped();
                continue;
            }

            await ingestor.IngestAsync(tick, ct); // без валидации тика — принимаем что распарсилось
        }
    }
}

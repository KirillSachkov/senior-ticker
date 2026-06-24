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
        // Оборачиваем цикл приёма в Polly-политику переподключения: любой сбой ведёт к reconnect,
        // а отмена (штатная остановка) пробрасывается и выходит из цикла.
        var pipeline = ResiliencePipelineFactory.CreateReconnectPipeline(options, (ex, delay, attempt) =>
            logger.LogWarning(ex, "{Name}: reconnect attempt {Attempt} in {Delay}", Name, attempt + 1, delay));
        try
        {
            await pipeline.ExecuteAsync(async token => await ConnectAndConsumeAsync(token), ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("{Name}: stopped (shutdown)", Name);
        }
    }

    private async Task ConnectAndConsumeAsync(CancellationToken ct)
    {
        // Шаг 1. Создаём сокет и подключаемся с таймаутом на connect.
        using var socket = socketFactory();
        await ConnectWithTimeoutAsync(socket, ct);
        logger.LogInformation("{Name}: connected to {Url}", Name, options.Url);

        // Шаг 2. Цикл приёма: один проход = одно сообщение, пока не попросили остановиться.
        var receiver = new WebSocketMessageReceiver(options.InitialReceiveBufferBytes, options.MaxMessageBytes);
        while (!ct.IsCancellationRequested)
        {
            // Шаг 2а. Читаем одно полное сообщение (с idle-таймаутом). Сервер закрыл соединение: бросаем
            // исключение, его поймает Polly и переподключится.
            using var msg = await ReceiveWithIdleTimeoutAsync(receiver, socket, ct);
            if (msg.IsClosed)
            {
                logger.LogInformation("{Name}: server closed connection — reconnecting", Name);
                throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely); // → Polly reconnect
            }

            // Шаг 2б. Парсим кадр в Tick. Парсер кинул исключение или не смог распарсить: считаем дроп
            // и идём к следующему сообщению, соединение не рвём.
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

            // Шаг 2в. Проверяем значения. Невалидный тик дропаем. Валидный отдаём в конвейер: на полном
            // входном канале IngestAsync ждёт (backpressure).
            if (!parsed) { metrics.OnDropped(); continue; }
            if (!TickValidator.IsValid(tick, time)) { metrics.OnDropped(); continue; }
            await ingestor.IngestAsync(tick, ct);
        }
    }

    /// <summary>ConnectAsync с дедлайном (WS-2). Таймаут → TimeoutException (не shutdown-OCE) → Polly reconnect.</summary>
    private async Task ConnectWithTimeoutAsync(ClientWebSocket socket, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(options.ConnectTimeout);
        try
        {
            await socket.ConnectAsync(options.Url, cts.Token);
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
            return await receiver.ReceiveAsync(socket, cts.Token);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new TimeoutException($"{Name}: no WS data within idle timeout {options.ReceiveIdleTimeout}");
        }
    }
}

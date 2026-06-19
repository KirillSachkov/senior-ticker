using System.Net.WebSockets;
using Microsoft.Extensions.Logging.Abstractions;
using SeniorTicker.Infrastructure.WebSockets;
using SeniorTicker.Infrastructure.WebSockets.Parsers;

namespace SeniorTicker.Integration.Tests;

public class ConnectorReconnectTests
{
    [Fact]
    public async Task Connector_auto_reconnects_after_server_drops_connection()
    {
        await using var server = await MockExchangeServer.StartAsync();
        var ingestor = new CollectingIngestor();

        // dropAfter=3: сервер закрывает соединение после 3 сообщений; наивный цикл должен переподключиться
        var options = new WebSocketConnectorOptions
        {
            Name = "binance",
            Url = new Uri($"{server.BaseWsUrl}/binance?dropAfter=3"),
        };
        var connector = new WebSocketConnectorBase(
            options,
            new BinanceMessageParser(),
            ingestor,
            new NoopMetrics(),
            TimeProvider.System,
            NullLogger.Instance,
            () => new ClientWebSocket());

        using var cts = new CancellationTokenSource();
        var run = connector.RunAsync(cts.Token);

        // без reconnect поток встал бы на 3 тиках — проверяем заметно больше
        await ConnectorReceivesAndNormalizesTests.WaitUntil(
            () => ingestor.Count >= 9,
            TimeSpan.FromSeconds(15));
        await cts.CancelAsync();
        await run;

        Assert.True(ingestor.Count >= 9,
            $"expected >=9 ticks across multiple reconnects, got {ingestor.Count}");
    }
}

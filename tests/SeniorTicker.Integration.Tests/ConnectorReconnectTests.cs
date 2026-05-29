using System.Net.WebSockets;
using Microsoft.Extensions.Logging.Abstractions;
using SeniorTicker.Domain;
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

        // dropAfter=3 → server closes after 3 messages; connector must reconnect and accumulate more
        var options = new WebSocketConnectorOptions
        {
            Name = "binance",
            Exchange = Exchange.Binance,
            Url = new Uri($"{server.BaseWsUrl}/binance?dropAfter=3"),
            ReconnectBaseDelay = TimeSpan.FromMilliseconds(50),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(200),
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

        // Without reconnect, the stream would stop at 3 ticks — we assert well above that
        await ConnectorReceivesAndNormalizesTests.WaitUntil(
            () => ingestor.Count >= 9,
            TimeSpan.FromSeconds(15));
        await cts.CancelAsync();
        await run;

        Assert.True(ingestor.Count >= 9,
            $"expected >=9 ticks across multiple reconnects, got {ingestor.Count}");
    }
}

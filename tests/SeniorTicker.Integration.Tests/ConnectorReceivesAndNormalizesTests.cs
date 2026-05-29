using System.Collections.Concurrent;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using SeniorTicker.MockExchange;

namespace SeniorTicker.Integration.Tests;

internal sealed class CollectingIngestor : ITickIngestor
{
    private readonly ConcurrentQueue<Tick> _t = new();
    public ValueTask IngestAsync(Tick tick, CancellationToken ct) { _t.Enqueue(tick); return ValueTask.CompletedTask; }
    public IReadOnlyCollection<Tick> Ticks => _t.ToArray();
    public int Count => _t.Count;
}

internal sealed class NoopMetrics : IMetricsSink
{
    public void OnReceived(long n = 1) { }
    public void OnDeduplicated(long n = 1) { }
    public void OnWritten(long n) { }
    public void OnDropped(long n = 1) { }
}

/// <summary>Starts MockExchange on a real free localhost port via Kestrel.</summary>
internal sealed class MockExchangeServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    public string BaseWsUrl { get; }

    private MockExchangeServer(WebApplication app, string baseWsUrl)
    {
        _app = app;
        BaseWsUrl = baseWsUrl;
    }

    public static async Task<MockExchangeServer> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var app = MockExchangeApp.Build(builder);
        await app.StartAsync();

        // Read the bound address from IServerAddressesFeature (app.Urls may be empty with port 0)
        var addresses = app.Services
            .GetRequiredService<IServer>()
            .Features
            .Get<IServerAddressesFeature>()!
            .Addresses;

        var httpAddr = addresses.First();
        var wsAddr = httpAddr.Replace("http://", "ws://", StringComparison.Ordinal);
        return new MockExchangeServer(app, wsAddr);
    }

    public async ValueTask DisposeAsync() => await _app.DisposeAsync();
}

public class ConnectorReceivesAndNormalizesTests
{
    [Fact]
    public async Task Binance_connector_receives_and_normalizes_ticks()
    {
        await using var server = await MockExchangeServer.StartAsync();
        var ingestor = new CollectingIngestor();
        var options = new WebSocketConnectorOptions
        {
            Name = "binance",
            Exchange = Exchange.Binance,
            Url = new Uri($"{server.BaseWsUrl}/binance"),
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

        await WaitUntil(() => ingestor.Count >= 5, TimeSpan.FromSeconds(15));
        await cts.CancelAsync();
        await run;

        Assert.True(ingestor.Count >= 5, $"expected >=5 ticks, got {ingestor.Count}");
        var t = ingestor.Ticks.First();
        Assert.Equal(Exchange.Binance, t.Exchange);
        Assert.Equal("BTCUSDT", t.Symbol);
        Assert.True(t.Price > 0m);
    }

    internal static async Task WaitUntil(Func<bool> cond, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (cond()) return;
            await Task.Delay(50);
        }
        throw new TimeoutException($"Condition not met within {timeout}");
    }
}

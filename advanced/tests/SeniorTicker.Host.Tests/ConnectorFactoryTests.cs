using Microsoft.Extensions.Logging.Abstractions;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Host.Ingestion;

namespace SeniorTicker.Host.Tests;

public class ConnectorFactoryTests
{
    private sealed class NoopIngestor : ITickIngestor
    {
        public ValueTask IngestAsync(Tick tick, CancellationToken ct) => ValueTask.CompletedTask;
    }

    private sealed class NoopMetrics : IMetricsSink
    {
        public void OnReceived(long n = 1) { }
        public void OnDeduplicated(long n = 1) { }
        public void OnWritten(long n) { }
        public void OnDropped(long n = 1) { }
    }

    private static ConnectorFactory NewFactory()
        => new(new NoopIngestor(), new NoopMetrics(), TimeProvider.System, NullLoggerFactory.Instance);

    private static ExchangeConfig Ex(string name, ExchangeFormat fmt, string url, bool enabled = true)
        => new() { Name = name, Format = fmt, Url = url, Enabled = enabled };

    [Fact]
    public void Builds_only_enabled_connectors()
    {
        // #9: disabled источник не превращается в коннектор.
        var connectors = NewFactory().CreateEnabled(
        [
            Ex("binance", ExchangeFormat.Binance, "wss://a/ws"),
            Ex("kraken",  ExchangeFormat.Kraken,  "wss://b/ws"),
            Ex("debug",   ExchangeFormat.Csv,     "wss://c/ws", enabled: false),
        ]);

        Assert.Equal(2, connectors.Count);
        Assert.Equal(new[] { "binance", "kraken" }, connectors.Select(c => c.Name).ToArray());
    }

    [Fact]
    public void Builds_all_three_formats_without_throwing()
    {
        var connectors = NewFactory().CreateEnabled(
        [
            Ex("binance",  ExchangeFormat.Binance, "ws://127.0.0.1:5000/binance"),
            Ex("kraken",   ExchangeFormat.Kraken,  "ws://127.0.0.1:5000/kraken"),
            Ex("coinbase", ExchangeFormat.Csv,     "ws://127.0.0.1:5000/csv"),
        ]);

        Assert.Equal(3, connectors.Count);
    }

    [Fact]
    public void Rejects_insecure_url_defense_in_depth()
    {
        // даже если URL прошёл мимо validator'а — фабрика валит на сборке (#9 + кольцо 1).
        Assert.Throws<InvalidOperationException>(() => NewFactory().CreateEnabled(
        [
            Ex("evil", ExchangeFormat.Binance, "ws://stream.binance.com/ws"),
        ]));
    }

    [Fact]
    public void Empty_config_yields_no_connectors()
        => Assert.Empty(NewFactory().CreateEnabled([]));
}

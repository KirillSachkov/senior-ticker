using System.Text;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class BinanceMessageParserTests
{
    private static ReadOnlySpan<byte> Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static readonly DateTimeOffset Ingest = DateTimeOffset.UnixEpoch;

    [Fact]
    public void Parses_valid_binance_trade()
    {
        var parser = new BinanceMessageParser();
        var json = """{"s":"BTCUSDT","p":"50000.50","q":"0.25","T":1700000000000,"a":98765}""";
        Assert.True(parser.TryParse(Utf8(json), Ingest, out var tick));
        Assert.Equal(Exchange.Binance, tick.Exchange);
        Assert.Equal("BTCUSDT", tick.Symbol);
        Assert.Equal(50000.50m, tick.Price);
        Assert.Equal(0.25m, tick.Volume);
        Assert.Equal(98765, tick.SourceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), tick.ExchangeTimestamp);
        Assert.Equal(Ingest, tick.IngestTimestamp);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("""{"s":"BTCUSDT","p":"notanumber","q":"1","T":1,"a":1}""")]
    [InlineData("""{"s":null,"p":"1","q":"1","T":1,"a":1}""")]
    public void Rejects_malformed_or_incomplete(string raw)
    {
        var parser = new BinanceMessageParser();
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }
}

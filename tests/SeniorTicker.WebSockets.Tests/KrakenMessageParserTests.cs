using System.Text;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class KrakenMessageParserTests
{
    private static ReadOnlySpan<byte> Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static readonly DateTimeOffset Ingest = DateTimeOffset.UnixEpoch;

    [Fact]
    public void Parses_valid_kraken_trade()
    {
        // [channelId, [price, volume, time_seconds], "trade", "BTC/USD", tradeId]
        var parser = new KrakenMessageParser();
        var raw = """[42,["50000.5","0.25","1700000000.500"],"trade","BTC/USD",12345]""";
        Assert.True(parser.TryParse(Utf8(raw), Ingest, out var tick));
        Assert.Equal(Exchange.Kraken, tick.Exchange);
        Assert.Equal("BTC/USD", tick.Symbol);
        Assert.Equal(50000.5m, tick.Price);
        Assert.Equal(0.25m, tick.Volume);
        Assert.Equal(12345, tick.SourceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000500), tick.ExchangeTimestamp);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"not":"array"}""")]
    [InlineData("""[42,["x","0.25","1700000000.5"],"trade","BTC/USD",1]""")]
    [InlineData("""[42,["50000"],"trade","BTC/USD",1]""")]
    public void Rejects_malformed(string raw)
    {
        var parser = new KrakenMessageParser();
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }

    [Fact]
    public void Rejects_out_of_range_timestamp()
    {
        // timeSec so large that ms overflows the valid DateTimeOffset range
        var parser = new KrakenMessageParser();
        var raw = """[42,["50000.5","0.25","99999999999999999"],"trade","BTC/USD",1]""";
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }
}

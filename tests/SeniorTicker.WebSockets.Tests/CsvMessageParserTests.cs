using System.Text;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class CsvMessageParserTests
{
    private static ReadOnlySpan<byte> Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static readonly DateTimeOffset Ingest = DateTimeOffset.UnixEpoch;

    [Fact]
    public void Parses_valid_csv_line()
    {
        // symbol,price,volume,epochMs,id
        var parser = new CsvMessageParser();
        Assert.True(parser.TryParse(Utf8("ETHUSD,3000.25,1.5,1700000000000,777"), Ingest, out var tick));
        Assert.Equal(Exchange.Coinbase, tick.Exchange);
        Assert.Equal("ETHUSD", tick.Symbol);
        Assert.Equal(3000.25m, tick.Price);
        Assert.Equal(1.5m, tick.Volume);
        Assert.Equal(777, tick.SourceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), tick.ExchangeTimestamp);
    }

    [Theory]
    [InlineData("")]
    [InlineData("ETHUSD,3000.25,1.5")]                 // too few fields
    [InlineData("ETHUSD,bad,1.5,1700000000000,1")]     // price not a number
    [InlineData(",3000,1.5,1700000000000,1")]          // empty symbol
    [InlineData("ETHUSD,3000,1.5,1700000000000,777,extra")] // 6 fields — too many
    public void Rejects_malformed(string raw)
    {
        var parser = new CsvMessageParser();
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }

    [Fact]
    public void Rejects_out_of_range_timestamp()
    {
        // epochMs fits in long but is outside valid DateTimeOffset ms range (MaxUnixMs = 253402300799999)
        var parser = new CsvMessageParser();
        // 9223372036854775807 = long.MaxValue — fits long but way outside valid ms range
        Assert.False(parser.TryParse(Utf8("ETHUSD,3000.25,1.5,9223372036854775807,1"), Ingest, out _));
    }
}

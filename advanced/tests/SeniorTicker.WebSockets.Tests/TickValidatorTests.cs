using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class TickValidatorTests
{
    private static readonly DateTimeOffset Now = new(2026, 5, 29, 12, 0, 0, TimeSpan.Zero);

    private static Tick Make(decimal price = 100m, decimal volume = 1m,
        DateTimeOffset? ts = null, string symbol = "BTCUSDT")
        => new(Exchange.Binance, symbol, price, volume, ts ?? Now, 1, Now);

    private static FakeTimeProvider Time() => new(Now);

    [Fact] public void Accepts_a_sane_tick() => Assert.True(TickValidator.IsValid(Make(), Time()));

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Rejects_non_positive_price(decimal price)
        => Assert.False(TickValidator.IsValid(Make(price: price), Time()));

    [Fact] public void Rejects_negative_volume() => Assert.False(TickValidator.IsValid(Make(volume: -1m), Time()));
    [Fact] public void Rejects_empty_symbol() => Assert.False(TickValidator.IsValid(Make(symbol: ""), Time()));

    // SEC: значение с 21+ цифрой целой части прошло бы parser и нижний гейт, но уронило бы COPY в
    // numeric(38,18) (numeric field overflow) → фолт конвейера → краш хоста одним кадром (remote DoS).
    [Fact]
    public void Rejects_price_above_column_range()
        => Assert.False(TickValidator.IsValid(Make(price: 100_000_000_000_000_000_000m), Time())); // 10^20

    [Fact]
    public void Rejects_volume_above_column_range()
        => Assert.False(TickValidator.IsValid(Make(volume: 100_000_000_000_000_000_000m), Time()));

    [Fact]
    public void Accepts_large_but_in_range_value()
        => Assert.True(TickValidator.IsValid(Make(price: TickValidator.MaxValue, volume: TickValidator.MaxValue), Time()));

    // SEC-3: символ не должен отравлять окно дедупликации/шард-роутинг — ограничиваем длину и charset.
    [Fact]
    public void Rejects_overlong_symbol()
        => Assert.False(TickValidator.IsValid(Make(symbol: new string('A', TickValidator.MaxSymbolLength + 1)), Time()));

    [Theory]
    [InlineData("BTC USD")]   // пробел
    [InlineData("BTC;DROP")]  // спецсимвол
    [InlineData("BTC\t")] // управляющий (таб)
    [InlineData("бтс")]       // не-ASCII
    public void Rejects_symbol_with_invalid_chars(string symbol)
        => Assert.False(TickValidator.IsValid(Make(symbol: symbol), Time()));

    [Theory]
    [InlineData("BTCUSDT")]
    [InlineData("BTC/USD")]
    [InlineData("XBT-USD")]
    [InlineData("BTC_USD")]
    [InlineData("BTC.D")]
    public void Accepts_real_world_symbols(string symbol)
        => Assert.True(TickValidator.IsValid(Make(symbol: symbol), Time()));

    [Fact]
    public void Rejects_timestamp_far_in_the_past()
        => Assert.False(TickValidator.IsValid(Make(ts: Now - TimeSpan.FromDays(8)), Time()));

    [Fact]
    public void Rejects_timestamp_far_in_the_future()
        => Assert.False(TickValidator.IsValid(Make(ts: Now + TimeSpan.FromMinutes(2)), Time()));
}

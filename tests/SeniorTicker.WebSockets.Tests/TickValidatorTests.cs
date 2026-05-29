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

    [Fact]
    public void Rejects_timestamp_far_in_the_past()
        => Assert.False(TickValidator.IsValid(Make(ts: Now - TimeSpan.FromDays(8)), Time()));

    [Fact]
    public void Rejects_timestamp_far_in_the_future()
        => Assert.False(TickValidator.IsValid(Make(ts: Now + TimeSpan.FromMinutes(2)), Time()));
}

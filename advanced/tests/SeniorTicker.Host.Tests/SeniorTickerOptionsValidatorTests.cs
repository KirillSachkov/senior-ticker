using Microsoft.Extensions.Options;
using SeniorTicker.Host.Configuration;

namespace SeniorTicker.Host.Tests;

public class SeniorTickerOptionsValidatorTests
{
    private static ValidateOptionsResult Validate(params ExchangeConfig[] exchanges)
        => new SeniorTickerOptionsValidator().Validate(null, new SeniorTickerOptions { Exchanges = [.. exchanges] });

    private static ExchangeConfig Ex(string name, string url, bool enabled = true)
        => new() { Name = name, Url = url, Format = ExchangeFormat.Binance, Enabled = enabled };

    [Fact]
    public void Enabled_public_ws_fails_boot()
    {
        var result = Validate(Ex("binance", "ws://stream.binance.com/ws"));
        Assert.True(result.Failed);
    }

    [Fact]
    public void Enabled_wss_passes()
        => Assert.True(Validate(Ex("binance", "wss://stream.binance.com/ws")).Succeeded);

    [Fact]
    public void Enabled_loopback_ws_passes()
        => Assert.True(Validate(Ex("mock", "ws://127.0.0.1:5000/binance")).Succeeded);

    [Fact]
    public void Disabled_invalid_url_is_skipped()
    {
        // #9: disabled-коннектор не регистрируется, поэтому его URL не валидируется.
        Assert.True(Validate(Ex("debug", "ws://evil.example.com/ws", enabled: false)).Succeeded);
    }

    [Fact]
    public void Duplicate_enabled_names_fail()
    {
        var result = Validate(
            Ex("dup", "wss://a/ws"),
            Ex("dup", "wss://b/ws"));
        Assert.True(result.Failed);
    }

    [Fact]
    public void Enabled_empty_name_fails()
        => Assert.True(Validate(Ex("", "wss://a/ws")).Failed);

    [Fact]
    public void Empty_exchange_list_passes()
        => Assert.True(Validate().Succeeded);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_drain_timeout_fails_boot(int seconds)
    {
        // §5.4: drain <= 0 уронил бы каждый штатный shutdown в форс-abort (потеря буфера, риск #5).
        var options = new SeniorTickerOptions { Shutdown = new ShutdownConfig { DrainTimeoutSeconds = seconds } };
        Assert.True(new SeniorTickerOptionsValidator().Validate(null, options).Failed);
    }

    [Fact]
    public void Positive_drain_timeout_passes()
    {
        var options = new SeniorTickerOptions { Shutdown = new ShutdownConfig { DrainTimeoutSeconds = 30 } };
        Assert.True(new SeniorTickerOptionsValidator().Validate(null, options).Succeeded);
    }
}

using SeniorTicker.Host.Security;

namespace SeniorTicker.Host.Tests;

public class TransportSecurityTests
{
    [Theory]
    [InlineData("wss://stream.binance.com:9443/ws")]   // защищённый — всегда ok
    [InlineData("ws://127.0.0.1:5000/binance")]        // loopback ws — dev/mock ok
    [InlineData("ws://localhost:5000/csv")]            // localhost = loopback
    [InlineData("ws://[::1]:5000/kraken")]             // IPv6 loopback
    public void Accepts_secure_or_loopback(string url)
        => Assert.Null(TransportSecurity.Validate("x", url));

    [Theory]
    [InlineData("ws://stream.binance.com:9443/ws")]    // публичный ws — #9: debug в проде
    [InlineData("ws://10.0.0.5/feed")]                 // приватная сеть, но не loopback
    [InlineData("http://example.com")]                 // вообще не ws
    [InlineData("https://example.com")]
    [InlineData("not-a-uri")]
    public void Rejects_insecure_or_nonloopback(string url)
        => Assert.NotNull(TransportSecurity.Validate("x", url));

    [Fact]
    public void ValidateOrThrow_throws_on_public_ws()
        => Assert.Throws<InvalidOperationException>(
            () => TransportSecurity.ValidateOrThrow("binance", "ws://stream.binance.com/ws"));

    [Fact]
    public void ValidateOrThrow_returns_uri_on_wss()
        => Assert.Equal(new Uri("wss://x/ws"), TransportSecurity.ValidateOrThrow("x", "wss://x/ws"));
}

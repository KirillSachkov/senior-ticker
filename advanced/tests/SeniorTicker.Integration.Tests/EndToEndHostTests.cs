using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Npgsql;
using SeniorTicker.Host;
using Testcontainers.PostgreSql;

namespace SeniorTicker.Integration.Tests;

/// <summary>
/// End-to-end (спека §16 фаза 6): реальный MockExchange (3 формата) → весь Host (DI, миграции на
/// старте #10, коннекторы #9, конвейер, binary COPY) → реальный Postgres (Testcontainers). Доказывает,
/// что composition root штатно стартует (миграции до writers), принимает потоки всех трёх бирж и
/// штатно гасится двухфазным дренажом без рваной транзакции.
/// </summary>
public sealed class EndToEndHostTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine").Build();
    private MockExchangeServer _mock = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        _mock = await MockExchangeServer.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _mock.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task MockExchange_through_host_to_postgres_end_to_end()
    {
        var connectionString = _postgres.GetConnectionString();
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:Postgres"] = connectionString,
            // быстрый флаш, чтобы строки оседали за пару сотен мс
            ["Pipeline:BatchMaxDelayMs"] = "50",
            ["Pipeline:BatchMaxSize"] = "100",
            ["Shutdown:DrainTimeoutSeconds"] = "10",
            ["Metrics:IntervalSeconds"] = "1",
            ["Exchanges:0:Name"] = "mock-binance",
            ["Exchanges:0:Format"] = "Binance",
            ["Exchanges:0:Url"] = $"{_mock.BaseWsUrl}/binance",
            ["Exchanges:0:Enabled"] = "true",
            ["Exchanges:1:Name"] = "mock-kraken",
            ["Exchanges:1:Format"] = "Kraken",
            ["Exchanges:1:Url"] = $"{_mock.BaseWsUrl}/kraken",
            ["Exchanges:1:Enabled"] = "true",
            ["Exchanges:2:Name"] = "mock-coinbase",
            ["Exchanges:2:Format"] = "Csv",
            ["Exchanges:2:Url"] = $"{_mock.BaseWsUrl}/csv",
            ["Exchanges:2:Enabled"] = "true",
        });
        builder.AddSeniorTicker();

        await using var dataSource = new NpgsqlDataSourceBuilder(connectionString).Build();

        using (var host = builder.Build())
        {
            await host.StartAsync(); // включает миграции (#10) до старта writers
            await WaitUntil(async () => await CountAsync(dataSource) >= 20, TimeSpan.FromSeconds(30));
            await host.StopAsync(); // двухфазный дренаж (#5): коннекторы → Complete() → дослив COPY
        }

        var total = await CountAsync(dataSource);
        var exchanges = await DistinctExchangesAsync(dataSource);

        Assert.True(total >= 20, $"expected ticks persisted, got {total}");
        // smallint(enum): Binance=1, Kraken=2, Coinbase=3 (CSV-формат) — все три источника дошли
        Assert.Contains((short)1, exchanges);
        Assert.Contains((short)2, exchanges);
        Assert.Contains((short)3, exchanges);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource ds)
    {
        await using var conn = await ds.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<HashSet<short>> DistinctExchangesAsync(NpgsqlDataSource ds)
    {
        await using var conn = await ds.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("SELECT DISTINCT exchange FROM ticks", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        var result = new HashSet<short>();
        while (await reader.ReadAsync())
            result.Add(reader.GetInt16(0));
        return result;
    }

    private static async Task WaitUntil(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException($"Condition not met within {timeout}");
    }
}

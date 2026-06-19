using Npgsql;
using SeniorTicker.DemoHost.Demo;
using SeniorTicker.Domain;

namespace SeniorTicker.DemoHost.Tests;

[Collection("demo-postgres")]
public class DemoPostgresSinkTests(DemoPostgresFixture fx)
{
    [Fact]
    public async Task Creates_table_writes_rows_and_resets()
    {
        var db = new DemoDatabase(fx.DataSource);
        await db.EnsureCreatedAsync(CancellationToken.None);
        await db.ResetAsync(CancellationToken.None);

        var sink = new DemoPostgresSink(fx.DataSource, "test-mode", delayMs: 0);
        Tick[] ticks =
        [
            NewTick("BTCUSDT", 1),
            NewTick("ETHUSDT", 2),
        ];

        await sink.WriteBatchAsync(ticks, CancellationToken.None);

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using (var count = new NpgsqlCommand("SELECT count(*) FROM demo_ticks WHERE mode='test-mode'", conn))
            Assert.Equal(2L, (long)(await count.ExecuteScalarAsync())!);
        Assert.Equal(2, sink.Rows);

        await db.ResetAsync(CancellationToken.None);
        await using var afterReset = new NpgsqlCommand("SELECT count(*) FROM demo_ticks", conn);
        Assert.Equal(0L, (long)(await afterReset.ExecuteScalarAsync())!);
    }

    private static Tick NewTick(string symbol, long sourceId)
        => new(Exchange.Binance, symbol, 100m, 1m, DateTimeOffset.UnixEpoch.AddMilliseconds(sourceId),
            sourceId, DateTimeOffset.UnixEpoch);
}

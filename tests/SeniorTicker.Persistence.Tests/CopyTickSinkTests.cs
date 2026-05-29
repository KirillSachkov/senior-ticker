using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.Persistence.Postgres;
using Npgsql;
using Xunit;

namespace SeniorTicker.Persistence.Tests;

[Collection("postgres")]
public class CopyTickSinkTests(PostgresFixture fx)
{
    private static Tick Tick(string symbol, long sourceId, decimal price = 50000.25m)
        => new(Exchange.Binance, symbol, price, 0.5m,
               new DateTimeOffset(2026, 5, 29, 12, 0, 0, TimeSpan.Zero), sourceId,
               new DateTimeOffset(2026, 5, 29, 12, 0, 0, TimeSpan.Zero));

    [Fact]
    public async Task Writes_a_batch_and_rows_are_queryable_with_correct_values()
    {
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        Tick[] batch = [Tick("BTCUSDT", 1), Tick("ETHUSDT", 2, 3000.75m), Tick("BTCUSDT", 3)];

        await sink.WriteBatchAsync(batch, CancellationToken.None);

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using (var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn))
            Assert.Equal(3L, (long)(await count.ExecuteScalarAsync())!);

        await using var cmd = new NpgsqlCommand(
            "SELECT symbol, price, volume, source_id FROM ticks WHERE source_id=2", conn);
        await using var r = await cmd.ExecuteReaderAsync();
        Assert.True(await r.ReadAsync());
        Assert.Equal("ETHUSDT", r.GetString(0));
        Assert.Equal(3000.75m, r.GetDecimal(1));
        Assert.Equal(0.5m, r.GetDecimal(2));
        Assert.Equal(2L, r.GetInt64(3));
    }

    [Fact]
    public async Task Empty_batch_is_a_noop()
    {
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        await sink.WriteBatchAsync(ReadOnlyMemory<Tick>.Empty, CancellationToken.None);
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(0L, (long)(await count.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Large_batch_5000_rows_writes_via_single_copy()
    {
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        var batch = new Tick[5000];
        for (var i = 0; i < batch.Length; i++) batch[i] = Tick("BTCUSDT", i);
        await sink.WriteBatchAsync(batch, CancellationToken.None);
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(5000L, (long)(await count.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Concurrent_writers_each_own_connection_no_corruption()
    {
        // фикс #4: K параллельных вызовов, каждый берёт своё соединение из NpgsqlDataSource
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        var tasks = Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            var batch = new Tick[500];
            for (var i = 0; i < batch.Length; i++) batch[i] = Tick($"S{w}", w * 1000 + i);
            await sink.WriteBatchAsync(batch, CancellationToken.None);
        }));
        await Task.WhenAll(tasks);
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(4000L, (long)(await count.ExecuteScalarAsync())!);
    }
}

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
    public async Task Failed_copy_rolls_back_whole_batch_and_keeps_pool_healthy()
    {
        // COPY транзакционен: дубль по ux_ticks_dedup_key проваливает CompleteAsync → НИЧЕГО не оседает,
        // а соединение возвращается в пул чистым (следующая запись работает). Доказывает «громкий» backstop.
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        Tick[] dup = [Tick("BTCUSDT", 1), Tick("BTCUSDT", 1)]; // одинаковый составной ключ

        await Assert.ThrowsAsync<PostgresException>(() => sink.WriteBatchAsync(dup, CancellationToken.None));

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using (var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn))
            Assert.Equal(0L, (long)(await count.ExecuteScalarAsync())!); // весь батч откатан

        // пул здоров: соединение вернулось пригодным
        await sink.WriteBatchAsync(new[] { Tick("ETHUSDT", 9) }, CancellationToken.None);
        await using var count2 = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(1L, (long)(await count2.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Cancelled_token_writes_nothing()
    {
        // ct=abort из двухфазного дренажа (План 1) должен честно отменять запись; OCE не глотается.
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => sink.WriteBatchAsync(new[] { Tick("BTCUSDT", 1) }, cts.Token));

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(0L, (long)(await count.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Preserves_instant_for_non_utc_offset()
    {
        // DateTimeOffset → timestamptz хранит UTC-инстант; offset отбрасывается, но момент сохраняется
        // (ровно та семантика, по которой сравнивает in-memory TickKey).
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        var ts = new DateTimeOffset(2026, 5, 29, 15, 0, 0, TimeSpan.FromHours(3)); // = 12:00:00Z
        var tick = new Tick(Exchange.Kraken, "BTC/USD", 100m, 1m, ts, 7, ts);

        await sink.WriteBatchAsync(new[] { tick }, CancellationToken.None);

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("SELECT exchange_ts FROM ticks WHERE source_id=7", conn);
        var stored = (DateTime)(await cmd.ExecuteScalarAsync())!; // timestamptz → DateTime Kind=Utc
        Assert.Equal(ts.UtcDateTime, stored);
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

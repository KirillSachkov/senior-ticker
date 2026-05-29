using Npgsql;
using Xunit;

namespace SeniorTicker.Persistence.Tests;

[Collection("postgres")]
public class SchemaTests(PostgresFixture fx)
{
    [Fact]
    public async Task Migration_creates_ticks_table_with_unique_and_brin_indexes()
    {
        await using var conn = await fx.DataSource.OpenConnectionAsync();

        // таблица ticks существует (to_regclass возвращает имя, иначе NULL);
        // ::text — Npgsql не маппит pg-тип regclass на System.Object в ExecuteScalar
        await using (var cmd = new NpgsqlCommand(
            "SELECT to_regclass('public.ticks')::text", conn))
            Assert.Equal("ticks", await cmd.ExecuteScalarAsync() as string);

        // unique index по ключу дедупа существует
        await using (var cmd = new NpgsqlCommand(
            "SELECT 1 FROM pg_indexes WHERE tablename='ticks' AND indexname='ux_ticks_dedup_key'", conn))
            Assert.Equal(1, await cmd.ExecuteScalarAsync());

        // BRIN-индекс по времени существует и это именно brin
        await using (var cmd = new NpgsqlCommand(
            "SELECT am.amname FROM pg_class i JOIN pg_am am ON i.relam=am.oid WHERE i.relname='ix_ticks_exchange_ts_brin'", conn))
            Assert.Equal("brin", await cmd.ExecuteScalarAsync() as string);
    }

    [Fact]
    public async Task Unique_dedup_key_rejects_a_true_duplicate()
    {
        await fx.TruncateAsync();
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        const string insert =
            "INSERT INTO ticks (exchange,symbol,price,volume,exchange_ts,source_id,ingest_ts) " +
            "VALUES (1,'BTCUSDT',1,1,'2026-05-29T00:00:00Z',42,'2026-05-29T00:00:00Z')";
        await using (var c1 = new NpgsqlCommand(insert, conn)) await c1.ExecuteNonQueryAsync();
        await using var c2 = new NpgsqlCommand(insert, conn);
        await Assert.ThrowsAsync<PostgresException>(() => c2.ExecuteNonQueryAsync()); // backstop ловит дубль
    }
}

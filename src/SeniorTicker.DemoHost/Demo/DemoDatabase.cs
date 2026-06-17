using Npgsql;

namespace SeniorTicker.DemoHost.Demo;

public sealed class DemoDatabase(NpgsqlDataSource dataSource)
{
    public async Task EnsureCreatedAsync(CancellationToken ct)
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS demo_ticks (
                mode text NOT NULL,
                exchange smallint NOT NULL,
                symbol varchar(32) NOT NULL,
                price numeric(38,18) NOT NULL,
                volume numeric(38,18) NOT NULL,
                exchange_ts timestamptz NOT NULL,
                source_id bigint NOT NULL,
                ingest_ts timestamptz NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_demo_ticks_mode ON demo_ticks(mode);
            """;

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("TRUNCATE TABLE demo_ticks", conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

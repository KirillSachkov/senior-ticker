using Npgsql;
using NpgsqlTypes;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.DemoHost.Demo;

public sealed class DemoPostgresSink(NpgsqlDataSource dataSource, string mode, int delayMs) : ITickSink
{
    private const string CopyCommand =
        "COPY demo_ticks (mode, exchange, symbol, price, volume, exchange_ts, source_id, ingest_ts) FROM STDIN (FORMAT BINARY)";

    private long _rows;

    public long Rows => Interlocked.Read(ref _rows);

    public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        if (batch.IsEmpty) return;
        if (delayMs > 0) await Task.Delay(delayMs, ct);

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var writer = await conn.BeginBinaryImportAsync(CopyCommand, ct);

        for (var i = 0; i < batch.Length; i++)
        {
            var t = batch.Span[i];
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(mode, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((short)t.Exchange, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(t.Symbol, NpgsqlDbType.Varchar, ct);
            await writer.WriteAsync(t.Price, NpgsqlDbType.Numeric, ct);
            await writer.WriteAsync(t.Volume, NpgsqlDbType.Numeric, ct);
            await writer.WriteAsync(t.ExchangeTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(t.SourceId, NpgsqlDbType.Bigint, ct);
            await writer.WriteAsync(t.IngestTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct);
        }

        await writer.CompleteAsync(ct);
        Interlocked.Add(ref _rows, batch.Length);
    }
}

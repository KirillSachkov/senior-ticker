using Npgsql;
using NpgsqlTypes;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// Пишет батч тиков через Npgsql BINARY COPY. Connection-per-call из потокобезопасного NpgsqlDataSource —
/// нет общего соединения/DbContext между K writer-воркерами (фикс #4). COPY ~100x EF SaveChanges (§7).
/// Дедуп — выше по конвейеру (single-writer), поэтому в ticks приходят уникальные ключи; UNIQUE-индекс —
/// backstop (дубль, если он всё же дойдёт, провалит батч — это намеренно «громкий» сигнал, не тихая потеря).
/// </summary>
public sealed class CopyTickSink(NpgsqlDataSource dataSource) : ITickSink
{
    private const string CopyCommand =
        "COPY ticks (exchange, symbol, price, volume, exchange_ts, source_id, ingest_ts) FROM STDIN (FORMAT BINARY)";

    public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        if (batch.IsEmpty) return;

        await using var conn = await dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var writer = await conn.BeginBinaryImportAsync(CopyCommand, ct).ConfigureAwait(false);

        for (var i = 0; i < batch.Length; i++)
        {
            var t = batch.Span[i];
            await writer.StartRowAsync(ct).ConfigureAwait(false);
            await writer.WriteAsync((short)t.Exchange, NpgsqlDbType.Smallint, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.Symbol, NpgsqlDbType.Varchar, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.Price, NpgsqlDbType.Numeric, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.Volume, NpgsqlDbType.Numeric, ct).ConfigureAwait(false);
            // Npgsql binary COPY в timestamptz принимает DateTimeOffset ТОЛЬКО с offset 0 (иначе ArgumentException,
            // не-OCE → уронил бы write-path). ToUniversalTime() сохраняет инстант, нормализуя offset → робастность
            // к любому upstream-offset; ровно та instant-семантика, по которой сравнивает TickKey.
            await writer.WriteAsync(t.ExchangeTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.SourceId, NpgsqlDbType.Bigint, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.IngestTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct).ConfigureAwait(false);
        }

        await writer.CompleteAsync(ct).ConfigureAwait(false); // без Complete COPY откатывается
    }
}

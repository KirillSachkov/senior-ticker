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
        // Шаг 1. Пустой батч: писать нечего.
        if (batch.IsEmpty) return;

        // Шаг 2. Берём своё соединение из пула и открываем бинарный COPY. Соединение на каждый вызов,
        // поэтому K writer-воркеров пишут параллельно, без общего DbContext.
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var writer = await conn.BeginBinaryImportAsync(CopyCommand, ct);

        // Шаг 3. Пишем каждый тик одной строкой: StartRow, затем поля строго в порядке колонок из CopyCommand.
        for (var i = 0; i < batch.Length; i++)
        {
            var t = batch.Span[i];
            await writer.StartRowAsync(ct);
            await writer.WriteAsync((short)t.Exchange, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(t.Symbol, NpgsqlDbType.Varchar, ct);
            await writer.WriteAsync(t.Price, NpgsqlDbType.Numeric, ct);
            await writer.WriteAsync(t.Volume, NpgsqlDbType.Numeric, ct);
            // Npgsql binary COPY в timestamptz принимает DateTimeOffset ТОЛЬКО с offset 0 (иначе ArgumentException,
            // не-OCE → уронил бы write-path). ToUniversalTime() сохраняет инстант, нормализуя offset → робастность
            // к любому upstream-offset; ровно та instant-семантика, по которой сравнивает TickKey.
            await writer.WriteAsync(t.ExchangeTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(t.SourceId, NpgsqlDbType.Bigint, ct);
            await writer.WriteAsync(t.IngestTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct);
        }

        // Шаг 4. CompleteAsync фиксирует COPY. Без него вся пачка откатывается.
        await writer.CompleteAsync(ct);
    }
}

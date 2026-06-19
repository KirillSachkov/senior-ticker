using Microsoft.EntityFrameworkCore;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// «Простой вариант» записи: ОДИН общий <see cref="TickDbContext"/> на весь сервис и <c>SaveChanges</c>
/// на каждый тик. Антипример к <see cref="CopyTickSink"/> (соединение-на-пачку + binary COPY): под
/// параллельной записью общий контекст бросает «A second operation was started on this context», а даже
/// без гонок это запрос в БД на каждый тик. Включается тумблером <c>Pipeline:Mode=Naive</c>.
/// </summary>
public sealed class NaiveDbContextSink(IDbContextFactory<TickDbContext> factory) : ITickSink, IDisposable
{
    private readonly TickDbContext _db = factory.CreateDbContext(); // один на весь сервис — общий, не потокобезопасен

    public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var t = batch.Span[i];
            _db.Ticks.Add(new TickEntity
            {
                Exchange = t.Exchange,
                Symbol = t.Symbol,
                Price = t.Price,
                Volume = t.Volume,
                ExchangeTimestamp = t.ExchangeTimestamp,
                SourceId = t.SourceId,
                IngestTimestamp = t.IngestTimestamp,
            });
            await _db.SaveChangesAsync(ct); // запрос в БД на каждый тик
        }
    }

    public void Dispose() => _db.Dispose();
}

using Microsoft.EntityFrameworkCore;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// Наивная запись: один общий <see cref="TickDbContext"/> на весь сервис и <c>SaveChanges</c> на каждый
/// тик. Под параллельной записью общий контекст бросает «A second operation was started on this context»,
/// и даже без гонок это запрос в БД на каждый тик. В advanced запись идёт через CopyTickSink:
/// соединение на пачку и binary COPY.
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

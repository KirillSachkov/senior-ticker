using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// EF-сущность для генерации схемы ticks и редких чтений. НЕ используется на горячем пути записи
/// (там — binary COPY из доменного Tick). Отдельный класс, т.к. EF не маппит readonly record struct.
/// </summary>
public sealed class TickEntity
{
    public long Id { get; set; }                 // bigint identity (суррогат для удобства, не ключ дедупликации)
    public Exchange Exchange { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal Volume { get; set; }
    public DateTimeOffset ExchangeTimestamp { get; set; }
    public long SourceId { get; set; }
    public DateTimeOffset IngestTimestamp { get; set; }
}

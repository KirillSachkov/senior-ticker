namespace SeniorTicker.Domain;

/// <summary>
/// Нормализованный тик. readonly record struct (value type): ноль per-item heap-аллокаций
/// на горячем пути; хранится в Channel и батч-массиве инлайн, без боксинга. decimal для
/// цены/объёма — точные деньги без float-дрейфа.
/// </summary>
public readonly record struct Tick(
    Exchange Exchange,
    string Symbol,
    decimal Price,
    decimal Volume,
    DateTimeOffset ExchangeTimestamp,
    long SourceId,
    DateTimeOffset IngestTimestamp)
{
    public TickKey Key => new(Exchange, Symbol, ExchangeTimestamp, SourceId);
}

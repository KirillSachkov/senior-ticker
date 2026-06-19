namespace SeniorTicker.Domain;

/// <summary>
/// Точный составной ключ дедупликации. SourceId = нативный trade_id/update_id биржи,
/// либо ingest-присвоенный монотонный счётчик (тогда идемпотентность только в рамках процесса).
/// НЕ 32-битный хеш: два РАЗНЫХ тика в одну миллисекунду не схлопываются в "дубликат".
/// </summary>
public readonly record struct TickKey(
    Exchange Exchange,
    string Symbol,
    DateTimeOffset ExchangeTimestamp,
    long SourceId);

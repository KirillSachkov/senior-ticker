using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Приёмник батчей тиков (адаптер хранилища). Выражен только в доменных типах — никакого
/// Npgsql сквозь интерфейс, чтобы "сменить БД" был дроп-ин. Реализация — connection-per-writer.
/// </summary>
public interface ITickSink
{
    Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct);
}

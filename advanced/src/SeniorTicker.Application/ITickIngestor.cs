using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Producer-seam: коннекторы пушат нормализованные тики сюда. Host связывает реализацию
/// с входом конвейера (TickPipeline.Input). Держит Infrastructure.WebSockets независимым от Processing.
/// </summary>
public interface ITickIngestor
{
    ValueTask IngestAsync(Tick tick, CancellationToken ct);
}

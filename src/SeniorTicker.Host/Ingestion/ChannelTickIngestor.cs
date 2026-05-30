using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Ingestion;

/// <summary>
/// Producer-seam → вход конвейера. <c>WriteAsync</c> в bounded-канал даёт backpressure #1: при
/// отставании БД коннектор реже читает сокет (а не копит в RAM). Связывает Infra.WebSockets и
/// Processing, не создавая прямой ссылки между ними (оба знают только порт <see cref="ITickIngestor"/>).
/// </summary>
public sealed class ChannelTickIngestor(TickPipeline pipeline) : ITickIngestor
{
    public ValueTask IngestAsync(Tick tick, CancellationToken ct) => pipeline.Input.WriteAsync(tick, ct);
}

using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// Наивная обработка: тик обрабатывается на потоке коннектора — проверка по общей дедупликации и запись
/// по одному тику. Без очереди, backpressure и шардов: под нагрузкой запись блокирует поток приёма,
/// а общая дедупликация гоняется между коннекторами. В advanced это разносят Channels и батчи.
/// </summary>
public sealed class NaiveTickIngestor(IDeduplicator dedup, ITickSink sink, IMetricsSink metrics) : ITickIngestor
{
    public async ValueTask IngestAsync(Tick tick, CancellationToken ct)
    {
        metrics.OnReceived();
        if (dedup.IsDuplicate(tick))
        {
            metrics.OnDeduplicated();
            return;
        }

        await sink.WriteBatchAsync(new[] { tick }, ct); // запись ПО ТИКУ, на потоке приёма
        metrics.OnWritten(1);
    }
}

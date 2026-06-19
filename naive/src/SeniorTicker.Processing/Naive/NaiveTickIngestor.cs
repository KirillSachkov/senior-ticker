using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант в лоб»: тик обрабатывается прямо на потоке коннектора — проверка по ОБЩЕМУ
/// дедупу и запись по одному тику в БД. Ни очереди, ни backpressure, ни шардов: под нагрузкой
/// запись блокирует поток приёма, а общий дедуп гоняется между коннекторами. Это и есть провал,
/// который показывает наивный вариант (Channels и батчи появляются только в advanced-решении).
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

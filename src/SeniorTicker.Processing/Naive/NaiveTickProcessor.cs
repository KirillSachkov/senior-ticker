using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант» обработки: каждый коннектор обрабатывает тик прямо на своём потоке —
/// проверяет дубль в ОБЩЕМ дедупе и тут же пишет в sink по одному тику. Ни шардирования,
/// ни одного владельца состояния, ни батчей. Антипример к <see cref="TickPipeline"/>: под
/// нагрузкой несколько потоков одновременно дёргают общий дедуп и общий sink.
/// </summary>
public sealed class NaiveTickProcessor(IDeduplicator dedup, ITickSink sink)
{
    public async Task HandleAsync(Tick tick, CancellationToken ct = default)
    {
        if (dedup.IsDuplicate(tick))
            return;

        await sink.WriteBatchAsync(new[] { tick }, ct).ConfigureAwait(false);
    }
}

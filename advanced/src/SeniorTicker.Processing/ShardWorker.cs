using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Потребитель одного шарда: применяет single-writer дедуп и группирует уникальные тики
/// в батчи (флаш по размеру N ИЛИ по времени T — что раньше; неполный батч флашится на
/// завершении источника). Один экземпляр = один поток-потребитель (инвариант дедупа).
/// НЕ завершает выходной канал — он общий для всех шардов (его завершает TickPipeline).
/// </summary>
public sealed class ShardWorker(
    IDeduplicator dedup,
    int maxSize,
    TimeSpan maxDelay,
    TimeProvider time,
    IMetricsSink metrics)
{
    public async Task RunAsync(
        ChannelReader<Tick> source,
        ChannelWriter<Tick[]> sink,
        CancellationToken ct)
    {
        while (await source.WaitToReadAsync(ct))
        {
            var batch = new List<Tick>(maxSize);
            using var timerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var timer = Task.Delay(maxDelay, time, timerCts.Token);

            while (batch.Count < maxSize)
            {
                if (source.TryRead(out var tick))
                {
                    if (dedup.IsDuplicate(tick)) { metrics.OnDeduplicated(); continue; }
                    batch.Add(tick);
                    continue;
                }

                var ready = source.WaitToReadAsync(ct).AsTask();
                var winner = await Task.WhenAny(ready, timer);
                if (winner == timer) break;             // флаш по времени T
                if (!await ready) break; // источник завершён
            }

            timerCts.Cancel();
            await ObserveAsync(timer);

            if (batch.Count > 0)
                await sink.WriteAsync(batch.ToArray(), ct);
        }
    }

    private static async Task ObserveAsync(Task task)
    {
        try { await task; }
        catch (OperationCanceledException) { /* ожидаемо при отмене таймера */ }
    }
}

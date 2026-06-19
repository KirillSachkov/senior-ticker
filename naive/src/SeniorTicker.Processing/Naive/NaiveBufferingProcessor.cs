using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант» остановки: тики копятся в очереди, отдельный флашер пишет их пачкой раз в
/// интервал — на ОДНОМ общем токене. При остановке этот токен отменяет и ожидание, и запись, поэтому
/// накопленное не дописывается и теряется. Никаких Channel — наивный вариант их не использует;
/// корректная остановка (двухфазная: закрыть вход, дочитать, дописать) — в advanced-решении.
/// </summary>
public sealed class NaiveBufferingProcessor(ITickSink sink)
{
    private readonly ConcurrentQueue<Tick> _buffer = new();

    /// <summary>Коннектор кладёт тик в общую очередь.</summary>
    public void Add(Tick tick) => _buffer.Enqueue(tick);

    public async Task RunAsync(CancellationToken hostToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(50, hostToken); // тот же токен на ожидание и на запись

                var batch = new List<Tick>();
                while (_buffer.TryDequeue(out var tick))
                    batch.Add(tick);

                if (batch.Count > 0)
                    await sink.WriteBatchAsync(batch.ToArray(), hostToken);
            }
        }
        catch (OperationCanceledException)
        {
            // токен отменён на ожидании или записи: накопленная очередь не дописана и теряется
        }
    }
}

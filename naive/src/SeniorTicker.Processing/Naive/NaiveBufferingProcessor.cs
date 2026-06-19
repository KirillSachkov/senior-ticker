using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант» остановки: тики копятся в обычном списке, а отдельный флашер пишет накопленное
/// пачкой раз в интервал — и всё на ОДНОМ общем токене. При штатной остановке токен рубит и ожидание,
/// и запись: накопленный буфер не дописан = потеря данных. Никаких Channel — наивный вариант их не
/// использует; правильная остановка (двухфазный дренаж: закрыть вход → дочитать → дописать) — в advanced.
/// </summary>
public sealed class NaiveBufferingProcessor(ITickSink sink)
{
    private readonly List<Tick> _buffer = [];
    private readonly object _gate = new();

    /// <summary>Коннектор просто кладёт тик в общий буфер.</summary>
    public void Add(Tick tick)
    {
        lock (_gate)
            _buffer.Add(tick);
    }

    public async Task RunAsync(CancellationToken hostToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(50, hostToken); // один токен и на ожидание, и на запись

                Tick[] batch;
                lock (_gate)
                {
                    if (_buffer.Count == 0)
                        continue;
                    batch = _buffer.ToArray();
                    _buffer.Clear();
                }

                await sink.WriteBatchAsync(batch, hostToken);
            }
        }
        catch (OperationCanceledException)
        {
            // hostToken отменён на ожидании или записи → накопленный буфер потерян, не дописан.
        }
    }
}

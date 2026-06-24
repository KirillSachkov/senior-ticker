using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Потребитель одного шарда: применяет single-writer дедупликацию и группирует уникальные тики
/// в батчи (флаш по размеру N ИЛИ по времени T — что раньше; неполный батч флашится на
/// завершении источника). Один экземпляр = один поток-потребитель (инвариант дедупликации).
/// НЕ завершает выходной канал — он общий для всех шардов (его завершает TickPipeline).
/// </summary>
public sealed class ShardWorker(
    IDeduplicator dedup,
    int maxSize,        // N: максимум тиков в одной пачке
    TimeSpan maxDelay,  // T: максимум времени на сбор одной пачки
    TimeProvider time,
    IMetricsSink metrics)
{
    public async Task RunAsync(
        ChannelReader<Tick> source,
        ChannelWriter<Tick[]> sink,
        CancellationToken ct)
    {
        // Внешний цикл: одна итерация = одна пачка. WaitToReadAsync ждёт, пока в шард-канале появится
        // хотя бы один тик; когда канал закрыт и пуст, возвращает false, и воркер выходит.
        while (await source.WaitToReadAsync(ct))
        {
            // Шаг 1. Заводим новую пустую пачку и запускаем таймер дедлайна T на её сбор.
            var batch = new List<Tick>(maxSize);
            using var timerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var timer = Task.Delay(maxDelay, time, timerCts.Token);

            // Шаг 2. Наполняем пачку, пока в ней меньше N тиков.
            while (batch.Count < maxSize)
            {
                // Шаг 2а. Пробуем взять тик без ожидания. Повтор пропускаем (и считаем метрику), уникальный
                // кладём в пачку и сразу идём за следующим: так забираем всё, что уже лежит в канале.
                if (source.TryRead(out var tick))
                {
                    if (dedup.IsDuplicate(tick)) { metrics.OnDeduplicated(); continue; }
                    batch.Add(tick);
                    continue;
                }

                // Шаг 2б. В канале сейчас пусто. Ждём, что наступит раньше: придёт новый тик (ready) или
                // сработает таймер T. Таймер раньше: выходим и флашим что набрали. ready вернул false: канал
                // закрыт, выходим. ready вернул true: тик пришёл, продолжаем цикл, его заберёт TryRead.
                var ready = source.WaitToReadAsync(ct).AsTask();
                var winner = await Task.WhenAny(ready, timer);
                if (winner == timer) break;
                if (!await ready) break;
            }

            // Шаг 3. Пачка готова (набрали N, вышло время T или закрылся канал). Гасим таймер и поглощаем его
            // задачу, чтобы не осталось необработанного исключения. Непустую пачку пишем массивом в общий батч-канал.
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

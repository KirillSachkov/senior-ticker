using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Дедуп по скользящему окну времени. SINGLE-WRITER: дёргается из одного потребителя →
/// HashSet/Queue без синхронизации (ноль локов, нет CAS). Точный TickKey (не 32-битный хеш).
/// Эвикция — O(1) амортизированно через FIFO-очередь сроков. Корректность порядка эвикции
/// ПРЕДПОЛАГАЕТ монотонно неубывающий источник времени (TimeProvider); при шаге часов назад
/// возможно раннее вытеснение (over-admit, не схлопывание).
/// </summary>
public sealed class SlidingWindowDeduplicator : IDeduplicator
{
    private readonly TimeSpan _window;
    private readonly TimeProvider _time;
    private readonly HashSet<TickKey> _seen = [];
    private readonly Queue<(DateTimeOffset Expiry, TickKey Key)> _expiry = new();

    public SlidingWindowDeduplicator(TimeSpan window, TimeProvider time)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        _window = window;
        _time = time;
    }

    public bool IsDuplicate(in Tick tick)
    {
        var now = _time.GetUtcNow();
        Evict(now);

        if (!_seen.Add(tick.Key))
            return true; // уже в окне

        _expiry.Enqueue((now + _window, tick.Key));
        return false;
    }

    private void Evict(DateTimeOffset now)
    {
        while (_expiry.TryPeek(out var head) && head.Expiry <= now)
        {
            _expiry.Dequeue();
            _seen.Remove(head.Key);
        }
    }
}

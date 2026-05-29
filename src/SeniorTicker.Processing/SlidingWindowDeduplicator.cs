using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Дедуп по скользящему окну времени. SINGLE-WRITER: дёргается из одного потребителя →
/// HashSet/Queue без синхронизации (ноль локов, нет CAS). Точный TickKey (не 32-битный хеш).
/// Эвикция — O(1) амортизированно через FIFO-очередь сроков: время приёма монотонно
/// неубывающее, поэтому expiry в очереди строго упорядочены.
/// </summary>
public sealed class SlidingWindowDeduplicator(TimeSpan window, TimeProvider time) : IDeduplicator
{
    private readonly HashSet<TickKey> _seen = [];
    private readonly Queue<(DateTimeOffset Expiry, TickKey Key)> _expiry = new();

    public bool IsDuplicate(in Tick tick)
    {
        var now = time.GetUtcNow();
        Evict(now);

        if (!_seen.Add(tick.Key))
            return true; // уже в окне

        _expiry.Enqueue((now + window, tick.Key));
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

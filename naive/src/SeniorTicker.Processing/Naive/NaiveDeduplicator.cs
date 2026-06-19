using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// Наивный дедуп: одна общая структура на все потоки. Отсюда три проблемы. Словарь, кольцо и индекс
/// меняются разными атомарными операциями — между ними проходит гонка (две атомарные операции не
/// образуют одной транзакции). Ключ HashCode.Combine(Symbol, ExchangeTimestamp) — 32-битный и без
/// SourceId, поэтому склеивает разные события. Переполнение int-индекса даёт отрицательный слот.
/// В advanced дедуп принадлежит одному потоку (HashSet без локов) и берёт точный TickKey.
/// </summary>
public sealed class NaiveDeduplicator : IDeduplicator
{
    private readonly ConcurrentDictionary<int, DateTime> _seen = new();
    private readonly int[] _ring;
    private int _i = -1;

    public NaiveDeduplicator(int ringSize = 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ringSize);
        _ring = new int[ringSize];
    }

    public bool IsDuplicate(in Tick tick)
    {
        var key = HashCode.Combine(tick.Symbol, tick.ExchangeTimestamp); // SourceId потерян

        if (!_seen.TryAdd(key, DateTime.UtcNow))
            return true;

        var slot = Interlocked.Increment(ref _i) % _ring.Length;
        var evicted = Interlocked.Exchange(ref _ring[slot], key);
        if (evicted != 0)
            _seen.TryRemove(evicted, out _);

        return false;
    }
}

using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант» дедупа: ОДНА общая структура на все потоки. Антипример к
/// <see cref="SlidingWindowDeduplicator"/> — здесь намеренно нарушены сразу три вещи:
/// (1) словарь, кольцо и индекс меняются разными атомарными операциями — между ними влезает
///     гонка (две атомарные операции не образуют одной транзакции);
/// (2) ключ — 32-битный <see cref="HashCode.Combine{T1,T2}"/>(Symbol, ExchangeTimestamp), который
///     ИГНОРИРУЕТ SourceId и склеивает разные события (коллизия → ложный дубль);
/// (3) <see cref="Interlocked.Increment(ref int)"/> по int-индексу при переполнении даёт
///     отрицательный слот.
/// Сделан «как в лоб», чтобы показать, почему дедупликация должна иметь одного владельца.
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

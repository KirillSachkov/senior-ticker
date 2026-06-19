using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// Наивная дедупликация: одна общая структура на все потоки. Отсюда три проблемы.
/// 1. Гонка: словарь, кольцо и индекс меняются тремя разными атомарными операциями; между ними
///    вклинивается другой поток, и окно рассинхронизируется (три атомарные операции — не одна транзакция).
/// 2. Коллизия: ключ HashCode.Combine(Symbol, ExchangeTimestamp) — 32-битный хеш без Exchange и SourceId,
///    поэтому два разных события могут дать один ключ, и второе примем за дубль.
/// 3. Переполнение: _i растёт без предела; когда int переполнится, _i % _ring.Length даёт отрицательный слот.
/// В advanced дедупликация принадлежит одному потоку: HashSet по точному TickKey, без локов и без хеша как ключа.
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
        // 32-битный хеш без Exchange и SourceId: коллизия → два разных события дадут один ключ
        var key = HashCode.Combine(tick.Symbol, tick.ExchangeTimestamp);

        if (!_seen.TryAdd(key, DateTime.UtcNow))
            return true; // ключ уже в окне → дубль

        // % заворачивает растущий индекс в кольцо; на переполнении int слот станет отрицательным
        var slot = Interlocked.Increment(ref _i) % _ring.Length;
        var evicted = Interlocked.Exchange(ref _ring[slot], key);
        if (evicted != 0)
            _seen.TryRemove(evicted, out _); // выкидываем самый старый ключ из словаря

        return false;
    }
}

using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// Наивная дедупликация: одна общая структура на все потоки. Отсюда три проблемы.
/// 1. Гонка: словарь, кольцо и индекс меняются тремя разными атомарными операциями; между ними
///    вклинивается другой поток, и окно рассинхронизируется (три атомарные операции — не одна транзакция).
/// 2. Коллизия: ключ HashCode.Combine(Symbol, ExchangeTimestamp) — 32-битный хеш без Exchange и SourceId,
///    поэтому два разных события могут дать один ключ, и второе примем за дубль. Добавить в хеш все поля
///    не спасает: 32 бита остаются 32 битами. Точный ключ — HashSet по самому TickKey, равенство сверяет все поля.
/// 3. Переполнение: _i растёт без предела; когда int переполнится, _i % _ring.Length даёт отрицательный слот.
/// В advanced дедупликация принадлежит одному потоку: HashSet по точному TickKey, без локов и без хеша как ключа.
/// </summary>
public sealed class NaiveDeduplicator : IDeduplicator
{
    // Множество увиденных ключей: ключ → когда добавлен. Сам по себе потокобезопасен.
    private readonly ConcurrentDictionary<int, DateTime> _seen = new();
    // Кольцевой буфер последних ringSize ключей — окно дедупликации. Старые ключи вытесняются по кругу.
    private readonly int[] _ring;
    // Счётчик позиции в кольце: растёт на каждый тик, по нему вычисляем слот.
    private int _i = -1;

    // ringSize — размер окна: сколько последних ключей помним. 1024 — небольшой дефолт; окно больше ловит
    // больше повторов, но требует больше памяти и поднимает шанс коллизии на 32-битном ключе.
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

        // Interlocked.Increment атомарно прибавляет 1 к _i и возвращает новое значение. ref передаёт само
        // поле по ссылке, чтобы менялось именно оно. % заворачивает индекс в кольцо; на переполнении int
        // значение уходит в минус, и слот становится отрицательным.
        var slot = Interlocked.Increment(ref _i) % _ring.Length;
        // Interlocked.Exchange кладёт key в слот и возвращает прежний ключ — это вытесненный (evicted) старый.
        var evicted = Interlocked.Exchange(ref _ring[slot], key);
        if (evicted != 0)
            _seen.TryRemove(evicted, out _); // убрали вытесненный ключ из словаря, чтобы окно не росло

        return false;
    }
}

using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Выбирает шард для тика по его ПОЛНОМУ ключу дедупликации (биржа, символ, метка времени, id).
/// Одинаковый ключ всегда даёт один и тот же индекс, поэтому повторы садятся в один шард и там
/// отбрасываются. Разные тики одного символа (другой id или время) расходятся по шардам и идут параллельно.
/// </summary>
public sealed class DedupKeyShardPartitioner : IShardPartitioner
{
    public int GetShard(in Tick tick, int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);

        // Шаг 1. Считаем стабильный хеш по всем четырём полям ключа: символ (FNV-1a по UTF-8 байтам),
        // затем подмешиваем биржу, метку времени и id источника.
        var hash = StableHash.Fnv1a64(tick.Symbol);
        hash = Mix(hash, (byte)tick.Exchange);
        hash = Mix(hash, unchecked((ulong)tick.ExchangeTimestamp.UtcDateTime.Ticks));
        hash = Mix(hash, unchecked((ulong)tick.SourceId));

        // Шаг 2. Остаток по числу шардов даёт индекс [0, shardCount). Хеш беззнаковый, поэтому индекс
        // никогда не отрицательный.
        return (int)(hash % (ulong)shardCount);
    }

    // FNV-смешивание: по очереди вмешиваем все 8 байт value в хеш (XOR очередного байта, умножение на FNV-prime).
    private static ulong Mix(ulong hash, ulong value)
    {
        const ulong prime = 1099511628211UL;

        for (var i = 0; i < sizeof(ulong); i++)
        {
            hash ^= (byte)(value >> (i * 8));
            hash *= prime;
        }

        return hash;
    }
}

using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

public sealed class DedupKeyShardPartitioner : IShardPartitioner
{
    public int GetShard(in Tick tick, int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);

        var hash = StableHash.Fnv1a64(tick.Symbol);
        hash = Mix(hash, (byte)tick.Exchange);
        hash = Mix(hash, unchecked((ulong)tick.ExchangeTimestamp.UtcDateTime.Ticks));
        hash = Mix(hash, unchecked((ulong)tick.SourceId));
        return (int)(hash % (ulong)shardCount);
    }

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

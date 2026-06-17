using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

public sealed class SymbolShardPartitioner : IShardPartitioner
{
    public int GetShard(in Tick tick, int shardCount)
        => StableHash.ShardOf(tick.Symbol, shardCount);
}

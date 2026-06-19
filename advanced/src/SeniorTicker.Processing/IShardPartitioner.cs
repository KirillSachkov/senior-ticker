using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

public interface IShardPartitioner
{
    int GetShard(in Tick tick, int shardCount);
}

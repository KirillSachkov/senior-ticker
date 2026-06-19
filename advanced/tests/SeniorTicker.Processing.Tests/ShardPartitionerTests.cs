using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Tests;

public class ShardPartitionerTests
{
    [Fact]
    public void Dedup_key_partitioner_spreads_one_hot_symbol_across_shards()
    {
        var partitioner = new DedupKeyShardPartitioner();
        const int shardCount = 8;

        var shards = Enumerable.Range(0, 10_000)
            .Select(i => partitioner.GetShard(TickFactory.New("BTCUSDT", i), shardCount))
            .Distinct()
            .ToArray();

        Assert.True(shards.Length > 1);
    }

    [Fact]
    public void Dedup_key_partitioner_routes_duplicate_key_to_same_shard()
    {
        var partitioner = new DedupKeyShardPartitioner();
        const int shardCount = 8;
        var tick = new Tick(Exchange.Binance, "BTCUSDT", 100m, 1m,
            DateTimeOffset.FromUnixTimeMilliseconds(123), 42, DateTimeOffset.UnixEpoch);

        var first = partitioner.GetShard(tick, shardCount);
        var second = partitioner.GetShard(tick, shardCount);

        Assert.Equal(first, second);
    }
}

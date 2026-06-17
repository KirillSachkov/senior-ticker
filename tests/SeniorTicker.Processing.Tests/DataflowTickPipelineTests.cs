using Microsoft.Extensions.Time.Testing;

namespace SeniorTicker.Processing.Tests;

public class DataflowTickPipelineTests
{
    private static DataflowTickPipeline Make(
        InMemoryTickSink sink,
        CountingMetricsSink metrics,
        IShardPartitioner? partitioner = null)
        => new(new PipelineOptions
        {
            ShardCount = 8,
            WriterCount = 2,
            IngestCapacity = 1024,
            ShardCapacity = 1024,
            BatchChannelCapacity = 8,
            BatchMaxSize = 100,
            BatchMaxDelay = TimeSpan.FromMilliseconds(50),
            DedupWindow = TimeSpan.FromMinutes(1),
        }, sink, metrics, new FakeTimeProvider(), partitioner ?? new DedupKeyShardPartitioner());

    [Fact]
    public async Task Deduplicates_hot_symbol_with_dedup_key_partitioning()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var pipeline = Make(sink, metrics);

        for (var i = 0; i < 10_000; i++)
        {
            var sourceId = i % 1_000;
            Assert.True(await pipeline.SendAsync(TickFactory.New("BTCUSDT", sourceId)));
        }

        pipeline.Complete();
        await pipeline.Completion.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(1_000, sink.All.Count);
        Assert.Equal(9_000, Interlocked.Read(ref metrics.Deduplicated));
        Assert.Equal(1_000, sink.All.Select(t => t.Key).Distinct().Count());
    }

    [Fact]
    public async Task Symbol_partitioning_mode_keeps_hot_symbol_correct_but_serialized()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var pipeline = Make(sink, metrics, new SymbolShardPartitioner());

        for (var i = 0; i < 100; i++)
            Assert.True(await pipeline.SendAsync(TickFactory.New("BTCUSDT", i % 10)));

        pipeline.Complete();
        await pipeline.Completion.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(10, sink.All.Count);
        Assert.Equal(90, Interlocked.Read(ref metrics.Deduplicated));
    }
}

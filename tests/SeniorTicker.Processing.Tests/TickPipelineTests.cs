using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class TickPipelineTests
{
    private static TickPipeline Make(InMemoryTickSink sink, CountingMetricsSink metrics,
        FakeTimeProvider time, int shards = 1, int writers = 2)
        => new(new PipelineOptions
        {
            ShardCount = shards,
            WriterCount = writers,
            BatchMaxSize = 100,
            BatchMaxDelay = TimeSpan.FromMilliseconds(50),
            DedupWindow = TimeSpan.FromMinutes(1),
        }, sink, metrics, time);

    [Fact]
    public async Task Writes_all_unique_ticks_and_drains_on_completion()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 4);

        var run = pipeline.RunAsync(CancellationToken.None);

        for (var i = 1; i <= 50; i++)
            await pipeline.Input.WriteAsync(TickFactory.New($"SYM{i % 5}", i));

        // двухфазный дренаж: завершаем вход и ждём полного прокачивания
        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(50, sink.All.Count);                 // ничего не потеряно при дренаже
        Assert.Equal(50, Interlocked.Read(ref metrics.Written));
    }

    [Fact]
    public async Task Deduplicates_repeated_ticks()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 2);

        var run = pipeline.RunAsync(CancellationToken.None);

        // один и тот же тик трижды + один уникальный
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 1));
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 1));
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 1));
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 2));

        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, sink.All.Count);
        Assert.Equal(2, Interlocked.Read(ref metrics.Deduplicated));
    }

    [Fact]
    public async Task Same_symbol_always_routes_to_one_shard_preserving_dedup()
    {
        // Дубликаты одного символа должны попасть в ОДИН шард → дедуп остаётся локальным.
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 8);

        var run = pipeline.RunAsync(CancellationToken.None);
        for (var i = 0; i < 10; i++)
            await pipeline.Input.WriteAsync(TickFactory.New("BTCUSDT", sourceId: 1)); // все дубликаты

        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(sink.All);                          // 1 уникальный, 9 отсеяно одним шардом
        Assert.Equal(9, Interlocked.Read(ref metrics.Deduplicated));
    }

    [Fact]
    public async Task Constructor_rejects_invalid_options()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new TickPipeline(new PipelineOptions { ShardCount = 0 }, sink, metrics, time));
    }
}

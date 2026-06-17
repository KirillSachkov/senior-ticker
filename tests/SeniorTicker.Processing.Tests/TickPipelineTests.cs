using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class TickPipelineTests
{
    private static TickPipeline Make(ITickSink sink, CountingMetricsSink metrics,
        FakeTimeProvider time, int shards = 1, int writers = 2, int batchMaxSize = 100,
        IShardPartitioner? partitioner = null)
        => new(new PipelineOptions
        {
            ShardCount = shards,
            WriterCount = writers,
            BatchMaxSize = batchMaxSize,
            BatchMaxDelay = TimeSpan.FromMilliseconds(50),
            DedupWindow = TimeSpan.FromMinutes(1),
        }, sink, metrics, time, partitioner);

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
    public async Task Dedup_key_partitioning_deduplicates_hot_symbol_across_shards()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 8, partitioner: new DedupKeyShardPartitioner());

        var run = pipeline.RunAsync(CancellationToken.None);
        for (var i = 0; i < 10_000; i++)
        {
            var sourceId = i % 1_000; // 10 repeats for every unique key
            await pipeline.Input.WriteAsync(TickFactory.New("BTCUSDT", sourceId));
        }

        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(1_000, sink.All.Count);
        Assert.Equal(9_000, Interlocked.Read(ref metrics.Deduplicated));
        Assert.Equal(1_000, sink.All.Select(t => t.Key).Distinct().Count());
    }

    [Fact]
    public async Task RunAsync_faults_when_sink_throws_and_does_not_hang()
    {
        var sink = new ThrowingTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        // batchMaxSize:1 → каждый уникальный тик = свой батч. 50 батчей при BatchChannelCapacity=8
        // и дохлых writer'ах: шард упирается в полный батч-канал и без фикса висит навсегда.
        var pipeline = Make(sink, metrics, time, shards: 2, writers: 2, batchMaxSize: 1);

        var run = pipeline.RunAsync(CancellationToken.None);
        for (var i = 0; i < 50; i++)
            await pipeline.Input.WriteAsync(TickFactory.New($"S{i % 3}", i));
        pipeline.Input.Complete();

        // Must FAULT with the sink's exception within a bounded time — not hang.
        // If it hangs, WaitAsync throws TimeoutException and ThrowsAsync<InvalidOperationException> fails.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => run.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Drains_all_ticks_without_loss_under_backpressure()
    {
        // TEST-3 / проблема №5: крошечные ёмкости + закрытый sink → ВСЕ каналы насыщаются и продюсер
        // паркуется на backpressure. После Open() + Complete() двухфазный дренаж обязан доставить всё.
        var sink = new GatedTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = new TickPipeline(new PipelineOptions
        {
            ShardCount = 1,
            WriterCount = 1,
            IngestCapacity = 2,
            ShardCapacity = 2,
            BatchChannelCapacity = 1,
            BatchMaxSize = 1,                                  // каждый тик = свой батч (без таймера)
            BatchMaxDelay = TimeSpan.FromMilliseconds(50),
            DedupWindow = TimeSpan.FromMinutes(1),
        }, sink, metrics, time);

        var run = pipeline.RunAsync(CancellationToken.None);

        const int n = 20;
        var produce = Task.Run(async () =>
        {
            for (var i = 0; i < n; i++)
                await pipeline.Input.WriteAsync(TickFactory.New("BTC", i)); // уникальные sourceId
            pipeline.Input.Complete();
        });

        await Task.Delay(100);                                 // дать конвейеру насытиться
        Assert.False(produce.IsCompleted, "ожидался backpressure: продюсер должен застрять на полном канале");

        sink.Open();                                           // открываем — бэклог дренажируется
        await produce;
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(n, sink.All.Count);                       // ноль потерь при дренаже под насыщением
        Assert.Equal(n, sink.All.Select(t => t.Key).Distinct().Count());
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

    [Fact]
    public async Task High_parallel_producer_load_no_lost_or_duplicate_writes()
    {
        // Много продюсеров параллельно шлют пересекающиеся ключи; роутинг по символу
        // гарантирует, что дубликаты сходятся в один шард → дедуп корректен без локов.
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 8, writers: 4);

        var run = pipeline.RunAsync(CancellationToken.None);

        const int producers = 16, perProducer = 500, uniqueKeys = 1000;
        var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
            {
                var id = (p * perProducer + i) % uniqueKeys; // намеренные пересечения ключей
                await pipeline.Input.WriteAsync(TickFactory.New($"S{id % 20}", id));
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(30));

        // каждый (symbol=S{id%20}, sourceId=id) уникален; всего uniqueKeys уникальных ключей
        Assert.Equal(uniqueKeys, sink.All.Count);
        Assert.Equal(uniqueKeys, sink.All.Select(t => t.Key).Distinct().Count());
    }
}

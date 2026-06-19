using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class ShardWorkerTests
{
    private static (Channel<Tick> src, Channel<Tick[]> sink) MakeChannels()
        => (Channel.CreateBounded<Tick>(1000), Channel.CreateBounded<Tick[]>(64));

    [Fact]
    public async Task Flushes_a_full_batch_by_size_N()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var worker = new ShardWorker(dedup, maxSize: 3, maxDelay: TimeSpan.FromMinutes(10), time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        for (var i = 1; i <= 3; i++)
            await src.Writer.WriteAsync(TickFactory.New("BTC", i));

        var batch = await sink.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, batch.Length);

        src.Writer.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Flushes_a_partial_batch_by_time_T()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var maxDelay = TimeSpan.FromMilliseconds(100);
        var worker = new ShardWorker(dedup, maxSize: 1000, maxDelay, time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1)); // 1 тик, N не достигнут

        var batch = await ReadWithTimeAdvanceAsync(sink.Reader, time, maxDelay);
        Assert.Single(batch);

        src.Writer.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Skips_duplicates_and_counts_them()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var worker = new ShardWorker(dedup, maxSize: 2, maxDelay: TimeSpan.FromMinutes(10), time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1));
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1)); // дубликат — пропустить
        await src.Writer.WriteAsync(TickFactory.New("BTC", 2));

        var batch = await sink.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, batch.Length);                 // только уникальные 1 и 2
        Assert.Equal(1, Interlocked.Read(ref metrics.Deduplicated));

        src.Writer.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Flushes_partial_batch_on_source_completion()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var worker = new ShardWorker(dedup, maxSize: 1000, maxDelay: TimeSpan.FromMinutes(10), time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1));
        await src.Writer.WriteAsync(TickFactory.New("BTC", 2));
        src.Writer.Complete();                          // источник завершён до заполнения N

        var batch = await sink.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, batch.Length);                  // неполный батч дофлашен
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Детерминированно дожидается батча, флашируемого по таймеру: продвигает FakeTimeProvider
    /// и уступает поток, пока воркер не дойдёт до await на таймере и не сработает флаш.
    /// </summary>
    private static async Task<Tick[]> ReadWithTimeAdvanceAsync(
        ChannelReader<Tick[]> reader, FakeTimeProvider time, TimeSpan step)
    {
        for (var i = 0; i < 200; i++)
        {
            if (reader.TryRead(out var b)) return b;
            time.Advance(step);
            await Task.Delay(1);
        }
        throw new TimeoutException("батч по таймеру не пришёл");
    }
}

using System.Runtime.CompilerServices;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Processing;

namespace SeniorTicker.Processing.Tests;

/// <summary>
/// Доказательная база §7: при целевой нагрузке система не теряет тики, а burst поглощается backpressure
/// (константная память, независимая от входной скорости — §5.3), а не копится в RAM. Дискриминирующая
/// проверка struct-дизайна — sub-LOH размер батч-массива (Assert batchBytes &lt; 85_000); gen2-дельта —
/// лишь грубый smoke-гейт «нет GC-шторма» (низкий gen2 ⇒ нет промотируемого retained-набора, что
/// необходимо, но НЕ достаточно для доказательства zero-per-item-heap). Точный профиль аллокаций —
/// ручной dotnet-counters (см. README).
/// </summary>
public class LoadTests
{
    private sealed class CountingSink : ITickSink
    {
        private long _written;
        public long Written => Interlocked.Read(ref _written);
        public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        {
            Interlocked.Add(ref _written, batch.Length); // без per-tick аллокаций — меряем только конвейер
            return Task.CompletedTask;
        }
    }

    private static PipelineOptions LoadOptions() => new()
    {
        ShardCount = 1,
        WriterCount = 2,
        IngestCapacity = 4096,
        ShardCapacity = 4096,
        BatchChannelCapacity = 16,
        BatchMaxSize = 900,                          // §7.2: 900 × sizeof(Tick) < 85 000 (граница LOH)
        BatchMaxDelay = TimeSpan.FromMilliseconds(50),
        DedupWindow = TimeSpan.FromMinutes(5),
    };

    [Fact]
    public async Task Sustained_high_throughput_no_loss_and_low_gen2()
    {
        const int total = 100_000;
        var sink = new CountingSink();
        var metrics = new CountingMetricsSink();
        var pipeline = new TickPipeline(LoadOptions(), sink, metrics, TimeProvider.System);

        var run = pipeline.RunAsync(CancellationToken.None);
        var gen2Before = GC.CollectionCount(2);

        for (long i = 0; i < total; i++)
            await pipeline.Input.WriteAsync(TickFactory.New("BTCUSDT", i)); // уникальный SourceId → не дубль
        pipeline.Input.Complete();
        await run;

        var gen2Delta = GC.CollectionCount(2) - gen2Before;
        var batchBytes = Unsafe.SizeOf<Tick>() * 900;

        Assert.Equal(total, sink.Written);                  // ноль потери
        Assert.Equal(total, metrics.Written);
        Assert.Equal(0, metrics.Deduplicated);              // все уникальны
        // Дискриминирующая проверка: батч-массив держится ниже LOH (struct-размер тут реально важен).
        Assert.True(batchBytes < 85_000,
            $"batch array {batchBytes}B must stay below LOH 85_000 (sizeof(Tick)={Unsafe.SizeOf<Tick>()})");
        // Грубый smoke-гейт «нет GC-шторма» (не доказательство zero-per-item-heap — см. summary; цель §7.3).
        Assert.True(gen2Delta <= 3,
            $"gen2 collections grew by {gen2Delta} over {total} ticks (sizeof(Tick)={Unsafe.SizeOf<Tick>()}, batch={batchBytes}B)");
    }

    [Fact]
    public async Task Burst_is_absorbed_by_backpressure_then_drains_without_loss()
    {
        const int burst = 60_000;                            // > суммарной ёмкости каналов (~24k) → продюсер встанет
        var sink = new GatedTickSink();                      // «БД висит»
        var metrics = new CountingMetricsSink();
        var pipeline = new TickPipeline(LoadOptions(), sink, metrics, TimeProvider.System);
        var run = pipeline.RunAsync(CancellationToken.None);

        var produce = Task.Run(async () =>
        {
            for (long i = 0; i < burst; i++)
                await pipeline.Input.WriteAsync(TickFactory.New("BTCUSDT", i));
            pipeline.Input.Complete();
        });

        await Task.Delay(250);
        Assert.False(produce.IsCompleted);                   // bounded-каналы тормозят продюсера, не копят всё в RAM

        sink.Open();                                         // «БД ожила»
        await produce;
        await run;

        Assert.Equal(burst, sink.All.Count);                 // всё дренировано после backpressure — ноль потери
    }
}

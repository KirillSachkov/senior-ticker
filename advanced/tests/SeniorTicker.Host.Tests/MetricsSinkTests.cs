using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using SeniorTicker.Host.Observability;

namespace SeniorTicker.Host.Tests;

public class MetricsSinkTests
{
    [Fact]
    public async Task Concurrent_increments_are_not_lost()
    {
        // #7: счётчики дёргаются из роутера/шардов/writers одновременно. Counter<long>.Add потокобезопасен
        // по конструкции — стандартный инструмент аддитивен, ни один инкремент не теряется.
        using var sink = new MetricsSink();
        using var written = new MetricCollector<long>(sink.Meter, "ticker.written");
        const int workers = 16;
        const int perWorker = 10_000;

        await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < perWorker; i++)
                sink.OnWritten(1);
        })));

        Assert.Equal((long)workers * perWorker, written.GetMeasurementSnapshot().Sum(m => m.Value));
    }

    [Fact]
    public void Each_event_increments_its_own_counter()
    {
        // Каждое событие конвейера идёт в свой стандартный Counter. gap = received − written считается
        // снаружи (в самих метриках не хранится).
        using var sink = new MetricsSink();
        using var received = new MetricCollector<long>(sink.Meter, "ticker.received");
        using var deduplicated = new MetricCollector<long>(sink.Meter, "ticker.deduplicated");
        using var written = new MetricCollector<long>(sink.Meter, "ticker.written");
        using var dropped = new MetricCollector<long>(sink.Meter, "ticker.dropped");

        sink.OnReceived(5);
        sink.OnDeduplicated(2);
        sink.OnWritten(3);
        sink.OnDropped();

        Assert.Equal(5, received.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(2, deduplicated.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(3, written.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(1, dropped.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(2,
            received.GetMeasurementSnapshot().Sum(m => m.Value) - written.GetMeasurementSnapshot().Sum(m => m.Value));
    }
}

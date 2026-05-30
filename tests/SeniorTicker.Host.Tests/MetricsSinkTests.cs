using SeniorTicker.Host.Observability;

namespace SeniorTicker.Host.Tests;

public class MetricsSinkTests
{
    [Fact]
    public async Task Concurrent_increments_are_not_lost()
    {
        // #7: счётчики дёргаются из роутера/шардов/writers одновременно — Interlocked не теряет.
        var sink = new MetricsSink();
        const int workers = 16;
        const int perWorker = 10_000;

        await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < perWorker; i++)
            {
                sink.OnReceived();
                sink.OnWritten(1);
            }
        })));

        var snap = sink.Snapshot();
        Assert.Equal(workers * perWorker, snap.Received);
        Assert.Equal(workers * perWorker, snap.Written);
        Assert.Equal(0, snap.Gap);
    }

    [Fact]
    public void Snapshot_is_monotonic_and_non_resetting()
    {
        // #7: снимок НЕ сбрасывает счётчики (баг ученика — частичный reset рассинхронизировал метрики).
        var sink = new MetricsSink();
        sink.OnReceived(5);
        sink.OnDeduplicated(2);
        sink.OnWritten(3);
        sink.OnDropped(1);

        var first = sink.Snapshot();
        var second = sink.Snapshot();

        Assert.Equal(new MetricsSnapshot(5, 2, 3, 1), first);
        Assert.Equal(first, second); // повторный снимок идентичен — ничего не сброшено
        Assert.Equal(2, first.Gap);  // received(5) - written(3)
    }
}

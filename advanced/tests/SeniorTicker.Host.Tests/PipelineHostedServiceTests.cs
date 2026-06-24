using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Host.Observability;
using SeniorTicker.Host.Pipeline;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Tests;

/// <summary>
/// Покрывает центр §5.4 — оркестрацию shutdown в PipelineHostedService: чистый дренаж (нет потери),
/// drain-timeout → форс-abort (детерминированно через FakeTimeProvider) и fail-fast #C1 (фатал sink'а →
/// StopApplication + ре-сёрфейс для ненулевого exit). Без БД — реальный TickPipeline + тест-sink'и.
/// </summary>
public class PipelineHostedServiceTests
{
    private static PipelineOptions FastOptions() => new()
    {
        ShardCount = 1,
        WriterCount = 1,
        IngestCapacity = 200,
        ShardCapacity = 200,
        BatchChannelCapacity = 8,
        BatchMaxSize = 1,                              // флаш по размеру немедленно — независим от batch-таймера
        BatchMaxDelay = TimeSpan.FromMilliseconds(50),
        DedupWindow = TimeSpan.FromMinutes(1),
    };

    private static Tick Tick(long sourceId) => new(
        Exchange.Binance, "BTCUSDT", 50000m, 0.5m,
        new DateTimeOffset(2026, 5, 30, 12, 0, 0, TimeSpan.Zero), sourceId,
        new DateTimeOffset(2026, 5, 30, 12, 0, 0, TimeSpan.Zero));

    private static PipelineHostedService NewService(
        TickPipeline pipeline, FakeLifetime lifetime, TimeProvider time, int drainSeconds = 5)
        => new(pipeline, new ShutdownConfig { DrainTimeoutSeconds = drainSeconds },
               lifetime, time, NullLogger<PipelineHostedService>.Instance);

    [Fact]
    public async Task StopAsync_drains_all_buffered_ticks_without_loss()
    {
        // фаза ② §5.4: Input.Complete() → дренаж дописывает буфер; ничего не теряется при штатном стопе.
        var sink = new InMemoryTickSink();
        var pipeline = new TickPipeline(FastOptions(), sink, new MetricsSink(), TimeProvider.System);
        var lifetime = new FakeLifetime();
        using var svc = NewService(pipeline, lifetime, TimeProvider.System);

        await svc.StartAsync(CancellationToken.None);
        for (long i = 1; i <= 50; i++)
            await pipeline.Input.WriteAsync(Tick(i));        // уникальные SourceId → дедупликация не отбрасывает
        await svc.StopAsync(CancellationToken.None);

        Assert.Equal(50, sink.Total);
        Assert.False(lifetime.StopRequested);                // чистый путь — приложение не валилось
    }

    [Fact]
    public async Task Sink_fault_triggers_failfast_stop_application_and_resurfaces()
    {
        // #C1: мёртвый sink → конвейер фолтится → наблюдатель зовёт StopApplication, StopAsync поднимает фатал.
        var pipeline = new TickPipeline(FastOptions(), new ThrowingTickSink(), new MetricsSink(), TimeProvider.System);
        var lifetime = new FakeLifetime();
        using var svc = NewService(pipeline, lifetime, TimeProvider.System);

        await svc.StartAsync(CancellationToken.None);
        await pipeline.Input.WriteAsync(Tick(1));

        await WaitUntil(() => lifetime.StopRequested, TimeSpan.FromSeconds(5));  // fail-fast сработал

        await Assert.ThrowsAnyAsync<Exception>(() => svc.StopAsync(CancellationToken.None)); // ненулевой exit
    }

    [Fact]
    public async Task Drain_timeout_forces_abort_without_hanging()
    {
        // превышение DrainTimeoutSeconds → TimeoutException → ForceAbort (не зависание). Время — на FakeTimeProvider.
        var sink = new BlockingTickSink();
        var pipeline = new TickPipeline(FastOptions(), sink, new MetricsSink(), TimeProvider.System);
        var fakeTime = new FakeTimeProvider();
        var lifetime = new FakeLifetime();
        using var svc = NewService(pipeline, lifetime, fakeTime, drainSeconds: 30);

        await svc.StartAsync(CancellationToken.None);
        await pipeline.Input.WriteAsync(Tick(1));
        await sink.Entered.WaitAsync(TimeSpan.FromSeconds(5));     // writer заблокирован в sink → дренаж не завершить

        var stop = svc.StopAsync(CancellationToken.None);
        Assert.False(stop.IsCompleted);                           // ждём drain на fakeTime
        fakeTime.Advance(TimeSpan.FromSeconds(31));                // переходим drain → форс-abort
        await stop.WaitAsync(TimeSpan.FromSeconds(5));            // завершается: abort отменил блокирующий sink

        Assert.False(lifetime.StopRequested);                    // таймаут дренажа ≠ фатал приложения
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(20);
        }
        throw new TimeoutException($"Condition not met within {timeout}");
    }

    private sealed class InMemoryTickSink : ITickSink
    {
        private long _total;
        public long Total => Interlocked.Read(ref _total);
        public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        {
            Interlocked.Add(ref _total, batch.Length);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingTickSink : ITickSink
    {
        public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
            => throw new InvalidOperationException("sink is dead");
    }

    private sealed class BlockingTickSink : ITickSink
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Entered => _entered.Task;
        public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        {
            _entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);   // блок до отмены (abort)
        }
    }

    private sealed class FakeLifetime : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource _stopping = new();
        public bool StopRequested { get; private set; }
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => _stopping.Token;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication()
        {
            StopRequested = true;
            _stopping.Cancel();
        }
    }
}

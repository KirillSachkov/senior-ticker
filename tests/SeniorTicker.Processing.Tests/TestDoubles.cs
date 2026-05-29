using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Tests;

/// <summary>Собирает все записанные тики (потокобезопасно) для проверок.</summary>
public sealed class InMemoryTickSink : ITickSink
{
    private readonly ConcurrentQueue<Tick> _all = new();
    public int BatchCount;

    public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        Interlocked.Increment(ref BatchCount);
        foreach (var t in batch.Span)
            _all.Enqueue(t);
        return Task.CompletedTask;
    }

    public IReadOnlyCollection<Tick> All => _all.ToArray();
}

/// <summary>Всегда падает при записи батча — для проверки fail-fast вместо дедлока.</summary>
public sealed class ThrowingTickSink : ITickSink
{
    public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        => throw new InvalidOperationException("sink failure");
}

/// <summary>Не отдаёт записи, пока не вызван Open() — позволяет насытить каналы и проверить дренаж под backpressure.</summary>
public sealed class GatedTickSink : ITickSink
{
    private readonly TaskCompletionSource _open = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentQueue<Tick> _all = new();
    public void Open() => _open.TrySetResult();
    public IReadOnlyCollection<Tick> All => _all.ToArray();

    public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        await _open.Task.WaitAsync(ct).ConfigureAwait(false);
        foreach (var t in batch.Span)
            _all.Enqueue(t);
    }
}

public sealed class CountingMetricsSink : IMetricsSink
{
    public long Received, Deduplicated, Written, Dropped;
    public void OnReceived(long n = 1) => Interlocked.Add(ref Received, n);
    public void OnDeduplicated(long n = 1) => Interlocked.Add(ref Deduplicated, n);
    public void OnWritten(long n) => Interlocked.Add(ref Written, n);
    public void OnDropped(long n = 1) => Interlocked.Add(ref Dropped, n);
}

public static class TickFactory
{
    public static Tick New(string symbol, long sourceId, decimal price = 100m,
        DateTimeOffset? exchangeTs = null, Exchange exchange = Exchange.Mock)
        => new(exchange, symbol, price, 1m,
               exchangeTs ?? DateTimeOffset.UnixEpoch, sourceId, DateTimeOffset.UnixEpoch);
}

using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.WebSockets.Tests;

public sealed class CollectingIngestor : ITickIngestor
{
    private readonly ConcurrentQueue<Tick> _ticks = new();
    public ValueTask IngestAsync(Tick tick, CancellationToken ct)
    {
        _ticks.Enqueue(tick);
        return ValueTask.CompletedTask;
    }
    public IReadOnlyCollection<Tick> Ticks => _ticks.ToArray();
}

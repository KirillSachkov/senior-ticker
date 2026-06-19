using SeniorTicker.Application;

namespace SeniorTicker.DemoHost.Demo;

public sealed class DemoMetricsSink : IMetricsSink
{
    private long _received;
    private long _deduplicated;
    private long _written;
    private long _dropped;

    public void OnReceived(long n = 1) => Interlocked.Add(ref _received, n);
    public void OnDeduplicated(long n = 1) => Interlocked.Add(ref _deduplicated, n);
    public void OnWritten(long n) => Interlocked.Add(ref _written, n);
    public void OnDropped(long n = 1) => Interlocked.Add(ref _dropped, n);

    public (long Received, long Deduplicated, long Written, long Dropped) Snapshot()
        => (Interlocked.Read(ref _received),
            Interlocked.Read(ref _deduplicated),
            Interlocked.Read(ref _written),
            Interlocked.Read(ref _dropped));
}

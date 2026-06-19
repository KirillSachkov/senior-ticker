using SeniorTicker.Application;

namespace SeniorTicker.Host.Observability;

/// <summary>Снимок всех счётчиков, снятый «вместе» (см. <see cref="MetricsSink.Snapshot"/>).</summary>
public readonly record struct MetricsSnapshot(long Received, long Deduplicated, long Written, long Dropped)
{
    /// <summary>Ключевая величина под нагрузкой (§12): успевают ли writers за приёмом.</summary>
    public long Gap => Received - Written;
}

/// <summary>
/// Фикс #7: потокобезопасные <b>монотонные</b> счётчики (Interlocked, дёргаются из роутера/шардов/
/// writers). <see cref="Snapshot"/> читает все вместе и НИЧЕГО не сбрасывает — у ученича баг был в
/// частичном reset, рассинхронизировавшем метрики. Rate считается дельтами снаружи (пропущенный
/// scrape не корраптит суммы).
/// </summary>
public sealed class MetricsSink : IMetricsSink
{
    private long _received;
    private long _deduplicated;
    private long _written;
    private long _dropped;

    public void OnReceived(long n = 1) => Interlocked.Add(ref _received, n);
    public void OnDeduplicated(long n = 1) => Interlocked.Add(ref _deduplicated, n);
    public void OnWritten(long n) => Interlocked.Add(ref _written, n);
    public void OnDropped(long n = 1) => Interlocked.Add(ref _dropped, n);

    public MetricsSnapshot Snapshot() => new(
        Interlocked.Read(ref _received),
        Interlocked.Read(ref _deduplicated),
        Interlocked.Read(ref _written),
        Interlocked.Read(ref _dropped));
}

namespace SeniorTicker.Host.Configuration;

/// <summary>Период публикации снимка метрик (§12).</summary>
public sealed class MetricsConfig
{
    public int IntervalSeconds { get; init; } = 1;
}

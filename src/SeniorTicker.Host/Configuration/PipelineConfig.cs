using SeniorTicker.Processing;

namespace SeniorTicker.Host.Configuration;

/// <summary>
/// Config-дружелюбное зеркало <see cref="PipelineOptions"/>: длительности — целыми Ms/Seconds
/// (биндятся из JSON чище, чем TimeSpan-строки). <see cref="ToOptions"/> — единственная точка маппинга.
/// </summary>
public sealed class PipelineConfig
{
    public int ShardCount { get; init; } = 1;
    public int WriterCount { get; init; } = 2;
    public int IngestCapacity { get; init; } = 1000;
    public int ShardCapacity { get; init; } = 1000;
    public int BatchChannelCapacity { get; init; } = 8;
    public int BatchMaxSize { get; init; } = 1000;
    public int BatchMaxDelayMs { get; init; } = 100;
    public int DedupWindowSeconds { get; init; } = 60;

    public PipelineOptions ToOptions() => new()
    {
        ShardCount = ShardCount,
        WriterCount = WriterCount,
        IngestCapacity = IngestCapacity,
        ShardCapacity = ShardCapacity,
        BatchChannelCapacity = BatchChannelCapacity,
        BatchMaxSize = BatchMaxSize,
        BatchMaxDelay = TimeSpan.FromMilliseconds(BatchMaxDelayMs),
        DedupWindow = TimeSpan.FromSeconds(DedupWindowSeconds),
    };
}

namespace SeniorTicker.Processing;

/// <summary>Ручки конвейера. Дефолты — под нагрузку теста (100/с). P/K выкручены в минимум.</summary>
public sealed class PipelineOptions
{
    public int ShardCount { get; init; } = 1;            // P: число single-writer-лейнов дедупликации
    public int WriterCount { get; init; } = 2;           // K: число writer-воркеров
    public int IngestCapacity { get; init; } = 1000;     // bounded input (backpressure #1)
    public int ShardCapacity { get; init; } = 1000;      // bounded per-shard
    public int BatchChannelCapacity { get; init; } = 8;  // bounded batches (backpressure #2)
    public int BatchMaxSize { get; init; } = 900;        // N: флаш по размеру. 900×sizeof(Tick=88)=79.2KB < LOH 85KB (§7.2)
    public TimeSpan BatchMaxDelay { get; init; } = TimeSpan.FromMilliseconds(100); // T: флаш по времени
    public TimeSpan DedupWindow { get; init; } = TimeSpan.FromMinutes(1);
}

namespace SeniorTicker.DemoHost.Demo;

public sealed record DemoConfig
{
    public int RatePerSecond { get; init; } = 1_000;
    public int HotSymbolPercent { get; init; } = 95;
    public int DuplicatePercent { get; init; } = 10;
    public int ShardCount { get; init; } = 8;
    public int WriterCount { get; init; } = 2;
    public int BatchMaxSize { get; init; } = 500;
    public int SinkDelayMs { get; init; }

    public DemoConfig Normalize() => this with
    {
        RatePerSecond = Math.Clamp(RatePerSecond, 1, 50_000),
        HotSymbolPercent = Math.Clamp(HotSymbolPercent, 0, 100),
        DuplicatePercent = Math.Clamp(DuplicatePercent, 0, 90),
        ShardCount = Math.Clamp(ShardCount, 1, 32),
        WriterCount = Math.Clamp(WriterCount, 1, 16),
        BatchMaxSize = Math.Clamp(BatchMaxSize, 1, 900),
        SinkDelayMs = Math.Clamp(SinkDelayMs, 0, 250),
    };
}

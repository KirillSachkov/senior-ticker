namespace SeniorTicker.DemoHost.Demo;

public sealed record DemoSnapshot(
    bool Running,
    DemoConfig Config,
    DateTimeOffset Timestamp,
    ModeSnapshot[] Modes);

public sealed record ModeSnapshot(
    string Mode,
    string Name,
    bool Running,
    long Accepted,
    long Received,
    long Deduplicated,
    long Written,
    long DbRows,
    int IngestDepth,
    int BatchDepth,
    int[] ShardDepths,
    long[] ShardAccepted);

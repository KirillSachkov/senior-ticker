namespace SeniorTicker.DemoHost.Demo;

public static class DemoRunStates
{
    public const string Stopped = "stopped";
    public const string Starting = "starting";
    public const string Running = "running";
    public const string Stopping = "stopping";
    public const string Resetting = "resetting";
}

public sealed record DemoSnapshot(
    bool Running,
    string State,
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

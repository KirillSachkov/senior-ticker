namespace SeniorTicker.DemoHost.Demo;

public enum DemoMode
{
    ChannelsSymbol = 1,
    ChannelsDedupKey = 2,
    DataflowDedupKey = 3,
}

public static class DemoModeNames
{
    public static string ToWireName(this DemoMode mode) => mode switch
    {
        DemoMode.ChannelsSymbol => "channels-symbol",
        DemoMode.ChannelsDedupKey => "channels-dedup-key",
        DemoMode.DataflowDedupKey => "dataflow-dedup-key",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };

    public static string ToDisplayName(this DemoMode mode) => mode switch
    {
        DemoMode.ChannelsSymbol => "Каналы: шардинг по символу",
        DemoMode.ChannelsDedupKey => "Каналы: шардинг по ключу тика",
        DemoMode.DataflowDedupKey => "TPL Dataflow: шардинг по ключу тика",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };
}

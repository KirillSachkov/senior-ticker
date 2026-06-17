namespace SeniorTicker.DemoHost.Demo;

public enum DemoMode
{
    ChannelsSymbol = 1,
    ChannelsDedupKey = 2,
}

public static class DemoModeNames
{
    public static string ToWireName(this DemoMode mode) => mode switch
    {
        DemoMode.ChannelsSymbol => "channels-symbol",
        DemoMode.ChannelsDedupKey => "channels-dedup-key",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };

    public static string ToDisplayName(this DemoMode mode) => mode switch
    {
        DemoMode.ChannelsSymbol => "Каналы: шардинг по символу",
        DemoMode.ChannelsDedupKey => "Каналы: шардинг по ключу тика",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };
}

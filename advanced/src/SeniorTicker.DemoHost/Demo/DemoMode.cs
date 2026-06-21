namespace SeniorTicker.DemoHost.Demo;

public enum DemoMode
{
    ChannelsDedupKey = 2,
}

public static class DemoModeNames
{
    public static string ToWireName(this DemoMode mode) => mode switch
    {
        DemoMode.ChannelsDedupKey => "channels-dedup-key",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };

    public static string ToDisplayName(this DemoMode mode) => mode switch
    {
        DemoMode.ChannelsDedupKey => "Channels + дедупликация по ключу тика",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };
}

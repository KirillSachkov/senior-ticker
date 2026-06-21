using SeniorTicker.DemoHost.Demo;

namespace SeniorTicker.DemoHost.Tests;

public class DemoModeNamesTests
{
    [Fact]
    public void Display_names_are_russian_for_demo_ui()
    {
        Assert.Equal("Channels + дедупликация по ключу тика", DemoMode.ChannelsDedupKey.ToDisplayName());
    }
}

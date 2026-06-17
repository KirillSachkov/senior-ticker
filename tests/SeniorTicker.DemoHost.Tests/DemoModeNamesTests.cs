using SeniorTicker.DemoHost.Demo;

namespace SeniorTicker.DemoHost.Tests;

public class DemoModeNamesTests
{
    [Fact]
    public void Display_names_are_russian_for_demo_ui()
    {
        Assert.Equal("Каналы: шардинг по символу", DemoMode.ChannelsSymbol.ToDisplayName());
        Assert.Equal("Каналы: шардинг по ключу тика", DemoMode.ChannelsDedupKey.ToDisplayName());
        Assert.Equal("TPL Dataflow: шардинг по ключу тика", DemoMode.DataflowDedupKey.ToDisplayName());
    }
}

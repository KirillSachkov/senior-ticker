using SeniorTicker.Host.Configuration;

namespace SeniorTicker.Host.Tests;

public class PipelineConfigTests
{
    [Fact]
    public void ToOptions_maps_durations_and_counts()
    {
        var cfg = new PipelineConfig
        {
            ShardCount = 4,
            WriterCount = 3,
            IngestCapacity = 2000,
            ShardCapacity = 1500,
            BatchChannelCapacity = 16,
            BatchMaxSize = 2500,
            BatchMaxDelayMs = 250,
            DedupWindowSeconds = 120,
        };

        var opt = cfg.ToOptions();

        Assert.Equal(4, opt.ShardCount);
        Assert.Equal(3, opt.WriterCount);
        Assert.Equal(2000, opt.IngestCapacity);
        Assert.Equal(1500, opt.ShardCapacity);
        Assert.Equal(16, opt.BatchChannelCapacity);
        Assert.Equal(2500, opt.BatchMaxSize);
        Assert.Equal(TimeSpan.FromMilliseconds(250), opt.BatchMaxDelay);
        Assert.Equal(TimeSpan.FromSeconds(120), opt.DedupWindow);
    }

    [Fact]
    public void Defaults_match_test_profile()
    {
        // дефолты конфига = «ручки в минимум» под нагрузку теста (P=1, K=2).
        var opt = new PipelineConfig().ToOptions();
        Assert.Equal(1, opt.ShardCount);
        Assert.Equal(2, opt.WriterCount);
        Assert.Equal(TimeSpan.FromMilliseconds(100), opt.BatchMaxDelay);
        Assert.Equal(TimeSpan.FromSeconds(60), opt.DedupWindow);
    }
}

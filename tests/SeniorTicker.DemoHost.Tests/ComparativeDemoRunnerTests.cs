using SeniorTicker.DemoHost.Demo;

namespace SeniorTicker.DemoHost.Tests;

[Collection("demo-postgres")]
public class ComparativeDemoRunnerTests(DemoPostgresFixture fx)
{
    [Fact]
    public async Task Starts_all_three_modes_and_writes_to_real_postgres()
    {
        var db = new DemoDatabase(fx.DataSource);
        var runner = new ComparativeDemoRunner(db, fx.DataSource);

        await runner.ResetAsync(CancellationToken.None);
        await runner.StartAsync(new DemoConfig
        {
            RatePerSecond = 300,
            HotSymbolPercent = 100,
            DuplicatePercent = 20,
            ShardCount = 4,
            WriterCount = 2,
            BatchMaxSize = 50,
        }, CancellationToken.None);

        await Task.Delay(800);
        await runner.StopAsync(CancellationToken.None);

        var snapshot = runner.Snapshot();
        Assert.Equal(3, snapshot.Modes.Length);
        Assert.All(snapshot.Modes, mode =>
        {
            Assert.True(mode.Accepted > 0);
            Assert.True(mode.Written > 0);
            Assert.True(mode.DbRows > 0);
        });
    }

    [Fact]
    public async Task High_duplicate_percent_is_reflected_in_metrics()
    {
        var db = new DemoDatabase(fx.DataSource);
        var runner = new ComparativeDemoRunner(db, fx.DataSource);

        await runner.ResetAsync(CancellationToken.None);
        await runner.StartAsync(new DemoConfig
        {
            RatePerSecond = 1_000,
            HotSymbolPercent = 100,
            DuplicatePercent = 90,
            ShardCount = 4,
            WriterCount = 2,
            BatchMaxSize = 50,
        }, CancellationToken.None);

        await Task.Delay(1_200);
        await runner.StopAsync(CancellationToken.None);

        var snapshot = runner.Snapshot();
        Assert.All(snapshot.Modes, mode =>
        {
            var duplicateRatio = (double)mode.Deduplicated / mode.Accepted;
            Assert.InRange(duplicateRatio, 0.75, 0.98);
        });
    }
}

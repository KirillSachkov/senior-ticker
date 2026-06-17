using Npgsql;
using SeniorTicker.DemoHost.Demo;

namespace SeniorTicker.DemoHost.Tests;

[Collection("demo-postgres")]
public class ComparativeDemoRunnerTests(DemoPostgresFixture fx)
{
    [Fact]
    public async Task Starts_both_modes_and_writes_to_real_postgres()
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
        Assert.Equal(DemoRunStates.Stopped, snapshot.State);
        Assert.Equal(2, snapshot.Modes.Length);
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
        Assert.Equal(DemoRunStates.Stopped, snapshot.State);
        Assert.All(snapshot.Modes, mode =>
        {
            var duplicateRatio = (double)mode.Deduplicated / mode.Accepted;
            Assert.InRange(duplicateRatio, 0.75, 0.98);
        });
    }

    [Fact]
    public async Task Stop_keeps_metrics_consistent_under_backpressure()
    {
        var db = new DemoDatabase(fx.DataSource);
        var runner = new ComparativeDemoRunner(db, fx.DataSource);

        await runner.ResetAsync(CancellationToken.None);
        await runner.StartAsync(new DemoConfig
        {
            RatePerSecond = 40_000,
            HotSymbolPercent = 95,
            DuplicatePercent = 12,
            ShardCount = 13,
            WriterCount = 5,
            BatchMaxSize = 145,
            SinkDelayMs = 45,
        }, CancellationToken.None);

        await Task.Delay(700);
        await runner.StopAsync(CancellationToken.None);

        var snapshot = runner.Snapshot();
        Assert.Equal(DemoRunStates.Stopped, snapshot.State);
        Assert.All(snapshot.Modes, mode =>
        {
            Assert.Equal(mode.Accepted, mode.Received);
            Assert.Equal(mode.Accepted - mode.Deduplicated, mode.Written);
            Assert.Equal(mode.Written, mode.DbRows);
            Assert.Empty(mode.ShardDepths);
        });
    }

    [Fact]
    public async Task Reset_clears_database_and_visible_counters()
    {
        var db = new DemoDatabase(fx.DataSource);
        var runner = new ComparativeDemoRunner(db, fx.DataSource);

        await runner.ResetAsync(CancellationToken.None);
        await runner.StartAsync(new DemoConfig
        {
            RatePerSecond = 500,
            HotSymbolPercent = 100,
            DuplicatePercent = 10,
            ShardCount = 4,
            WriterCount = 2,
            BatchMaxSize = 50,
        }, CancellationToken.None);

        await Task.Delay(800);
        await runner.ResetAsync(CancellationToken.None);

        var snapshot = runner.Snapshot();
        Assert.False(snapshot.Running);
        Assert.Equal(DemoRunStates.Stopped, snapshot.State);
        Assert.All(snapshot.Modes, mode =>
        {
            Assert.Equal(0, mode.Accepted);
            Assert.Equal(0, mode.Received);
            Assert.Equal(0, mode.Deduplicated);
            Assert.Equal(0, mode.Written);
            Assert.Equal(0, mode.DbRows);
            Assert.All(mode.ShardAccepted, value => Assert.Equal(0, value));
        });

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM demo_ticks", conn);
        Assert.Equal(0L, (long)(await count.ExecuteScalarAsync())!);
    }
}

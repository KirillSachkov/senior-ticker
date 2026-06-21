using Microsoft.Extensions.Logging;
using Npgsql;
using SeniorTicker.Processing;

namespace SeniorTicker.DemoHost.Demo;

public sealed class ComparativeDemoRunner
{
    private readonly DemoDatabase _database;
    private readonly DemoModeRunner _runner;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ILogger<ComparativeDemoRunner>? _log;
    private DemoConfig _config = new();
    private string _state = DemoRunStates.Stopped;
    private bool _running;

    public ComparativeDemoRunner(
        DemoDatabase database,
        NpgsqlDataSource dataSource,
        ILogger<ComparativeDemoRunner>? log = null)
    {
        _database = database;
        _log = log;
        _runner = new DemoModeRunner(DemoMode.ChannelsDedupKey, dataSource, new DedupKeyShardPartitioner());
    }

    public async Task StartAsync(DemoConfig config, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            _state = DemoRunStates.Starting;
            await StopCoreAsync("restart-before-start", setStoppedState: false);
            _config = config.Normalize();
            _log?.LogInformation(
                "Demo starting: rate={RatePerSecond}, hot={HotSymbolPercent}%, duplicates={DuplicatePercent}%, shards={ShardCount}, writers={WriterCount}, batch={BatchMaxSize}, dbDelayMs={SinkDelayMs}",
                _config.RatePerSecond,
                _config.HotSymbolPercent,
                _config.DuplicatePercent,
                _config.ShardCount,
                _config.WriterCount,
                _config.BatchMaxSize,
                _config.SinkDelayMs);
            await _database.EnsureCreatedAsync(ct);
            await _runner.StartAsync(_config);
            _running = true;
            _state = DemoRunStates.Running;
            _log?.LogInformation("Demo running");
        }
        catch (Exception ex)
        {
            _running = false;
            _state = DemoRunStates.Stopped;
            _log?.LogError(ex, "Demo start failed");
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            _state = DemoRunStates.Stopping;
            await StopCoreAsync("stop", setStoppedState: true);
        }
        catch (Exception ex)
        {
            _log?.LogError(ex, "Demo stop failed");
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            _state = DemoRunStates.Resetting;
            await StopCoreAsync("reset-before-truncate", setStoppedState: false);
            await _database.EnsureCreatedAsync(ct);
            await _database.ResetAsync(ct);
            _runner.ResetStats(_config);
            _running = false;
            _state = DemoRunStates.Stopped;
            _log?.LogInformation("Demo database truncated and visible counters reset");
        }
        catch (Exception ex)
        {
            _running = false;
            _state = DemoRunStates.Stopped;
            _log?.LogError(ex, "Demo reset failed");
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public DemoSnapshot Snapshot()
        => new(_running, _state, _config, DateTimeOffset.UtcNow, [_runner.Snapshot()]);

    private async Task StopCoreAsync(string reason, bool setStoppedState)
    {
        await _runner.StopAsync();
        _running = false;
        if (setStoppedState)
            _state = DemoRunStates.Stopped;
        LogStopSummary(reason);
    }

    private void LogStopSummary(string reason)
    {
        var mode = _runner.Snapshot();
        if (mode.Accepted == 0 && mode.Written == 0 && mode.DbRows == 0)
            return;

        _log?.LogInformation(
            "Demo stopped ({Reason}): {Mode}: accepted={Accepted}, dedup={Deduplicated}, written={Written}, dbRows={DbRows}, shardAccepted=[{ShardAccepted}]",
            reason,
            mode.Mode,
            mode.Accepted,
            mode.Deduplicated,
            mode.Written,
            mode.DbRows,
            string.Join(",", mode.ShardAccepted));
    }
}

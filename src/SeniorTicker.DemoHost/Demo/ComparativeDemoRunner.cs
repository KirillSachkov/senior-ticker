using Npgsql;
using SeniorTicker.Processing;

namespace SeniorTicker.DemoHost.Demo;

public sealed class ComparativeDemoRunner
{
    private readonly DemoDatabase _database;
    private readonly DemoModeRunner[] _modes;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private DemoConfig _config = new();
    private bool _running;

    public ComparativeDemoRunner(DemoDatabase database, NpgsqlDataSource dataSource)
    {
        _database = database;
        _modes =
        [
            new(DemoMode.ChannelsSymbol, dataSource, new SymbolShardPartitioner(), useDataflow: false),
            new(DemoMode.ChannelsDedupKey, dataSource, new DedupKeyShardPartitioner(), useDataflow: false),
            new(DemoMode.DataflowDedupKey, dataSource, new DedupKeyShardPartitioner(), useDataflow: true),
        ];
    }

    public async Task StartAsync(DemoConfig config, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await StopCoreAsync();
            _config = config.Normalize();
            await _database.EnsureCreatedAsync(ct);
            foreach (var mode in _modes)
                await mode.StartAsync(_config);
            _running = true;
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
            await StopCoreAsync();
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
            await StopCoreAsync();
            await _database.EnsureCreatedAsync(ct);
            await _database.ResetAsync(ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    public DemoSnapshot Snapshot()
        => new(_running, _config, DateTimeOffset.UtcNow, _modes.Select(m => m.Snapshot()).ToArray());

    private async Task StopCoreAsync()
    {
        foreach (var mode in _modes)
            await mode.StopAsync();
        _running = false;
    }
}

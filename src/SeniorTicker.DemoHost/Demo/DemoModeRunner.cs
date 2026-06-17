using Npgsql;
using SeniorTicker.Domain;
using SeniorTicker.Processing;

namespace SeniorTicker.DemoHost.Demo;

public sealed class DemoModeRunner
{
    private static readonly string[] ColdSymbols = ["ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"];

    private readonly DemoMode _mode;
    private readonly NpgsqlDataSource _dataSource;
    private readonly IShardPartitioner _partitioner;
    private readonly bool _useDataflow;
    private readonly object _gate = new();

    private DemoConfig _config = new();
    private DemoMetricsSink _metrics = new();
    private DemoPostgresSink? _sink;
    private TickPipeline? _channels;
    private Task? _channelsTask;
    private DataflowTickPipeline? _dataflow;
    private CancellationTokenSource? _producerCts;
    private Task? _producerTask;
    private long _accepted;
    private long _lastUniqueKey = -1;
    private long[] _shardAccepted = [];

    public DemoModeRunner(DemoMode mode, NpgsqlDataSource dataSource, IShardPartitioner partitioner, bool useDataflow)
    {
        _mode = mode;
        _dataSource = dataSource;
        _partitioner = partitioner;
        _useDataflow = useDataflow;
    }

    public bool Running => _producerTask is { IsCompleted: false };

    public Task StartAsync(DemoConfig config)
    {
        lock (_gate)
        {
            _config = config.Normalize();
            _metrics = new DemoMetricsSink();
            _sink = new DemoPostgresSink(_dataSource, _mode.ToWireName(), _config.SinkDelayMs);
            _accepted = 0;
            _lastUniqueKey = -1;
            _shardAccepted = new long[_config.ShardCount];
            _producerCts = new CancellationTokenSource();

            var options = new PipelineOptions
            {
                ShardCount = _config.ShardCount,
                WriterCount = _config.WriterCount,
                IngestCapacity = 4096,
                ShardCapacity = 4096,
                BatchChannelCapacity = 16,
                BatchMaxSize = _config.BatchMaxSize,
                BatchMaxDelay = TimeSpan.FromMilliseconds(100),
                DedupWindow = TimeSpan.FromMinutes(5),
            };

            if (_useDataflow)
            {
                _dataflow = new DataflowTickPipeline(options, _sink, _metrics, TimeProvider.System, _partitioner);
                _channels = null;
            }
            else
            {
                _channels = new TickPipeline(options, _sink, _metrics, TimeProvider.System, _partitioner);
                _dataflow = null;
                _channelsTask = _channels.RunAsync(CancellationToken.None);
            }

            _producerTask = ProduceAsync(_producerCts.Token);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        CancellationTokenSource? cts;
        Task? producer;
        TickPipeline? channels;
        DataflowTickPipeline? dataflow;
        Task? channelsTask;

        lock (_gate)
        {
            cts = _producerCts;
            producer = _producerTask;
            channels = _channels;
            dataflow = _dataflow;
            channelsTask = _channelsTask;
            _producerCts = null;
            _producerTask = null;
            _channels = null;
            _channelsTask = null;
            _dataflow = null;
        }

        if (cts is not null)
            await cts.CancelAsync();

        if (producer is not null)
        {
            try { await producer; }
            catch (OperationCanceledException) { }
        }

        channels?.Input.TryComplete();
        dataflow?.Complete();

        if (channelsTask is not null)
            await channelsTask.WaitAsync(TimeSpan.FromSeconds(10));

        if (dataflow is not null)
            await dataflow.Completion.WaitAsync(TimeSpan.FromSeconds(10));

        cts?.Dispose();
    }

    public ModeSnapshot Snapshot()
    {
        var totals = _metrics.Snapshot();
        var channels = _channels;
        var dataflow = _dataflow;
        var shardAccepted = _shardAccepted.Select((_, i) => Interlocked.Read(ref _shardAccepted[i])).ToArray();

        return new ModeSnapshot(
            _mode.ToWireName(),
            _mode.ToDisplayName(),
            Running,
            Interlocked.Read(ref _accepted),
            totals.Received,
            totals.Deduplicated,
            totals.Written,
            _sink?.Rows ?? 0,
            channels?.IngestDepth ?? dataflow?.IngestDepth ?? 0,
            channels?.BatchDepth ?? dataflow?.BatchDepth ?? 0,
            channels?.ShardDepths ?? dataflow?.ShardDepths ?? [],
            shardAccepted);
    }

    private async Task ProduceAsync(CancellationToken ct)
    {
        var sequence = 0L;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));

        while (await timer.WaitForNextTickAsync(ct))
        {
            var config = _config;
            var count = Math.Max(1, config.RatePerSecond / 10);
            for (var i = 0; i < count; i++)
            {
                var tick = CreateTick(sequence++, config);
                var shard = _partitioner.GetShard(tick, config.ShardCount);

                if (_channels is not null)
                    await _channels.Input.WriteAsync(tick, ct);
                else if (_dataflow is not null)
                    await _dataflow.SendAsync(tick, ct);

                Interlocked.Increment(ref _accepted);
                Interlocked.Increment(ref _shardAccepted[shard]);
            }
        }
    }

    private Tick CreateTick(long sequence, DemoConfig config)
    {
        var duplicate = config.DuplicatePercent > 0
            && _lastUniqueKey >= 0
            && sequence % 100 < config.DuplicatePercent;
        var key = duplicate ? _lastUniqueKey : sequence;

        if (!duplicate)
            _lastUniqueKey = key;

        var symbol = key % 100 < config.HotSymbolPercent
            ? "BTCUSDT"
            : ColdSymbols[(int)(key % ColdSymbols.Length)];
        var ts = DateTimeOffset.UnixEpoch.AddMilliseconds(key);

        return new Tick(
            Exchange.Binance,
            symbol,
            50_000m + key % 10_000,
            1m,
            ts,
            key,
            DateTimeOffset.UtcNow);
    }
}

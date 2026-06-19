using System.Collections.Concurrent;
using System.Threading.Channels;
using Npgsql;
using SeniorTicker.Domain;

namespace SeniorTicker.DemoHost.Demo;

/// <summary>
/// Демо-режим «наивный вариант»: без шардов, общий дедуп и запись ПО ТИКУ (без батчей, без
/// backpressure). Под нагрузкой видно проблему: запись не успевает за приёмом — Written отстаёт
/// от Accepted, очередь (IngestDepth) растёт без границы. Контраст к шардированным каналам с
/// батч-записью. Самодостаточен — не зависит от боевого Processing.
/// </summary>
public sealed class NaiveDemoModeRunner(DemoMode mode, NpgsqlDataSource dataSource) : IDemoModeRunner
{
    private static readonly string[] ColdSymbols = ["ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"];

    private readonly object _gate = new();
    private DemoConfig _config = new();
    private DemoMetricsSink _metrics = new();
    private DemoPostgresSink? _sink;
    private CancellationTokenSource? _cts;
    private Task? _producerTask;
    private Task[] _handlers = [];
    private Channel<Tick>? _queue;
    private long _accepted;
    private long _lastUniqueKey = -1;

    // НАИВНЫЙ общий дедуп: словарь + кольцо вытеснения, без локов; 32-битный ключ теряет SourceId.
    private ConcurrentDictionary<int, byte> _seen = new();
    private int[] _ring = [];
    private int _ringIndex = -1;

    public bool Running => _producerTask is { IsCompleted: false };

    public void ResetStats(DemoConfig config)
    {
        lock (_gate)
        {
            _config = config.Normalize();
            _metrics = new DemoMetricsSink();
            _sink = null;
            _accepted = 0;
            _lastUniqueKey = -1;
        }
    }

    public Task StartAsync(DemoConfig config)
    {
        lock (_gate)
        {
            _config = config.Normalize();
            _metrics = new DemoMetricsSink();
            _sink = new DemoPostgresSink(dataSource, mode.ToWireName(), _config.SinkDelayMs);
            _accepted = 0;
            _lastUniqueKey = -1;
            _seen = new ConcurrentDictionary<int, byte>();
            _ring = new int[4096];
            _ringIndex = -1;
            _queue = Channel.CreateUnbounded<Tick>(); // без границы — наивно, нет backpressure
            _cts = new CancellationTokenSource();

            _handlers = Enumerable.Range(0, 2).Select(_ => HandleAsync(_cts.Token)).ToArray();
            _producerTask = ProduceAsync(_cts.Token);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        CancellationTokenSource? cts;
        Task? producer;
        Task[] handlers;
        Channel<Tick>? queue;

        lock (_gate)
        {
            cts = _cts;
            producer = _producerTask;
            handlers = _handlers;
            queue = _queue;
        }

        if (cts is not null)
            await cts.CancelAsync();

        if (producer is not null)
        {
            try { await producer; }
            catch (OperationCanceledException) { }
        }

        queue?.Writer.TryComplete();
        try { await Task.WhenAll(handlers); }
        catch { /* отмена/наивные ошибки записи */ }

        lock (_gate)
        {
            if (ReferenceEquals(_cts, cts)) _cts = null;
            if (ReferenceEquals(_producerTask, producer)) _producerTask = null;
        }

        cts?.Dispose();
    }

    public ModeSnapshot Snapshot()
    {
        var totals = _metrics.Snapshot();
        var accepted = Interlocked.Read(ref _accepted);
        var depth = _queue?.Reader.Count ?? 0; // растущий бэклог = наглядная проблема
        return new ModeSnapshot(
            mode.ToWireName(),
            mode.ToDisplayName(),
            Running,
            accepted,
            totals.Received,
            totals.Deduplicated,
            totals.Written,
            _sink?.Rows ?? 0,
            depth,
            0,
            [],
            [accepted]);
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
                ct.ThrowIfCancellationRequested();

                var tick = CreateTick(sequence++, config);
                var queue = _queue;
                if (queue is null)
                    return;

                await queue.Writer.WriteAsync(tick, ct); // unbounded: не ждёт, память растёт
                Interlocked.Increment(ref _accepted);
            }
        }
    }

    private async Task HandleAsync(CancellationToken ct)
    {
        var queue = _queue!;
        var sink = _sink!;
        try
        {
            await foreach (var tick in queue.Reader.ReadAllAsync(ct))
            {
                _metrics.OnReceived();
                if (IsDuplicate(tick))
                {
                    _metrics.OnDeduplicated();
                    continue;
                }

                await sink.WriteBatchAsync(new[] { tick }, ct); // запись ПО ТИКУ, без батча
                _metrics.OnWritten(1);
            }
        }
        catch (OperationCanceledException) { }
        catch { /* наивный путь: ошибки записи в демо глотаем, чтобы не валить процесс */ }
    }

    // НАИВНО: словарь + кольцо без локов — компаунд из неатомарных шагов, гонка может пропустить дубль.
    private bool IsDuplicate(in Tick tick)
    {
        var key = HashCode.Combine(tick.Symbol, tick.ExchangeTimestamp);
        if (!_seen.TryAdd(key, 0))
            return true;

        var slot = Interlocked.Increment(ref _ringIndex) % _ring.Length;
        if (slot < 0)
            slot = 0;
        var evicted = Interlocked.Exchange(ref _ring[slot], key);
        if (evicted != 0)
            _seen.TryRemove(evicted, out _);

        return false;
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

using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Конвейер обработки в памяти: input → router(stableHash(Symbol)%P) → P шардов
/// (single-writer дедуп + батч) → shared Channel&lt;Tick[]&gt; → K writer-воркеров → ITickSink.
/// Все каналы BOUNDED (FullMode.Wait) → явный backpressure и ограниченная память.
/// Двухфазный дренаж: Input.Complete() → router завершает шарды → шарды флашат остаток →
/// батч-канал завершается → writers дописывают → RunAsync возвращается. Отмена ct = abort.
/// </summary>
public sealed class TickPipeline
{
    private readonly PipelineOptions _opt;
    private readonly ITickSink _sink;
    private readonly IMetricsSink _metrics;
    private readonly TimeProvider _time;

    private readonly Channel<Tick> _ingest;
    private readonly Channel<Tick>[] _shards;
    private readonly Channel<Tick[]> _batches;

    public TickPipeline(PipelineOptions options, ITickSink sink, IMetricsSink metrics, TimeProvider time)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.ShardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.WriterCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.IngestCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.ShardCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchChannelCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchMaxSize);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.BatchMaxDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.DedupWindow, TimeSpan.Zero);

        _opt = options;
        _sink = sink;
        _metrics = metrics;
        _time = time;

        _ingest = Channel.CreateBounded<Tick>(new BoundedChannelOptions(options.IngestCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,   // один router читает вход
            SingleWriter = false,  // много коннекторов пишут
        });

        _shards = new Channel<Tick>[options.ShardCount];
        for (var i = 0; i < _shards.Length; i++)
        {
            _shards[i] = Channel.CreateBounded<Tick>(new BoundedChannelOptions(options.ShardCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,   // один ShardWorker = один поток-потребитель (инвариант дедупа)
                SingleWriter = true,   // один router пишет в шард
            });
        }

        _batches = Channel.CreateBounded<Tick[]>(new BoundedChannelOptions(options.BatchChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,  // K writers читают
            SingleWriter = false,  // P шардов пишут
        });
    }

    /// <summary>Точка входа для продюсеров (коннекторов). Backpressure #1.</summary>
    public ChannelWriter<Tick> Input => _ingest.Writer;

    public async Task RunAsync(CancellationToken ct)
    {
        var shardTasks = new Task[_shards.Length];
        for (var i = 0; i < _shards.Length; i++)
        {
            var dedup = new SlidingWindowDeduplicator(_opt.DedupWindow, _time);
            var worker = new ShardWorker(dedup, _opt.BatchMaxSize, _opt.BatchMaxDelay, _time, _metrics);
            shardTasks[i] = worker.RunAsync(_shards[i].Reader, _batches.Writer, ct);
        }

        var routerTask = RouteAsync(ct);

        // когда все шарды доедены — завершаем общий батч-канал, чтобы writers вышли
        var shardsCompletion = Task.Run(async () =>
        {
            await Task.WhenAll(shardTasks).ConfigureAwait(false);
            _batches.Writer.TryComplete();
        }, CancellationToken.None);

        var writerTasks = new Task[_opt.WriterCount];
        for (var i = 0; i < writerTasks.Length; i++)
            writerTasks[i] = WriteLoopAsync(ct);

        await Task.WhenAll(routerTask, shardsCompletion).ConfigureAwait(false);
        await Task.WhenAll(writerTasks).ConfigureAwait(false);
    }

    private async Task RouteAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var tick in _ingest.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                _metrics.OnReceived();
                var shard = _shards[StableHash.ShardOf(tick.Symbol, _shards.Length)];
                await shard.Writer.WriteAsync(tick, ct).ConfigureAwait(false); // backpressure #1
            }
        }
        finally
        {
            // вход завершён (или отмена) → завершаем все шард-каналы, чтобы воркеры дофлашили остаток
            foreach (var shard in _shards)
                shard.Writer.TryComplete();
        }
    }

    private async Task WriteLoopAsync(CancellationToken ct)
    {
        await foreach (var batch in _batches.Reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            await _sink.WriteBatchAsync(batch, ct).ConfigureAwait(false);
            _metrics.OnWritten(batch.Length);
        }
    }
}

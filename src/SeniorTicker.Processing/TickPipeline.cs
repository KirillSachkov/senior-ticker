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

    /// <summary>Глубина входного канала (для метрик §12: полный → лимитер дедуп/батч).</summary>
    public int IngestDepth => _ingest.Reader.Count;

    /// <summary>Глубина батч-канала (для метрик §12: полный → лимитер БД).</summary>
    public int BatchDepth => _batches.Reader.Count;

    public async Task RunAsync(CancellationToken ct)
    {
        // Внутренняя отмена, связанная с внешним ct: гасится либо внешней отменой, либо
        // падением writer'а (C1). Чистый дренаж её НЕ трогает — все воркеры выходят сами.
        using var abort = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var shardTasks = new Task[_shards.Length];
        for (var i = 0; i < _shards.Length; i++)
        {
            var dedup = new SlidingWindowDeduplicator(_opt.DedupWindow, _time);
            var worker = new ShardWorker(dedup, _opt.BatchMaxSize, _opt.BatchMaxDelay, _time, _metrics);
            shardTasks[i] = worker.RunAsync(_shards[i].Reader, _batches.Writer, abort.Token);
        }

        var routerTask = RouteAsync(abort.Token);

        // когда все шарды доедены (или упали) — завершаем общий батч-канал в finally,
        // чтобы writers вышли даже при фолте шарда (I1).
        var shardsCompletion = CompleteBatchesWhenShardsDoneAsync(shardTasks);

        var writerTasks = new Task[_opt.WriterCount];
        for (var i = 0; i < writerTasks.Length; i++)
            writerTasks[i] = WriteLoopAsync(abort.Token, abort);

        var all = new List<Task>(2 + writerTasks.Length) { routerTask, shardsCompletion };
        all.AddRange(writerTasks);
        try
        {
            await Task.WhenAll(all).ConfigureAwait(false);
        }
        catch
        {
            // Поверх отмены приоритетно поднимаем осмысленную ошибку (напр. фолт sink'а),
            // сохраняя её стек, а не OperationCanceledException-следствие.
            var fault = all.Where(t => t.IsFaulted)
                           .SelectMany(t => t.Exception!.InnerExceptions)
                           .FirstOrDefault(e => e is not OperationCanceledException);
            if (fault is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(fault).Throw();
            throw;
        }
    }

    private async Task CompleteBatchesWhenShardsDoneAsync(Task[] shardTasks)
    {
        try { await Task.WhenAll(shardTasks).ConfigureAwait(false); }
        finally { _batches.Writer.TryComplete(); }
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

    private async Task WriteLoopAsync(CancellationToken ct, CancellationTokenSource abort)
    {
        try
        {
            await foreach (var batch in _batches.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                await _sink.WriteBatchAsync(batch, ct).ConfigureAwait(false);
                _metrics.OnWritten(batch.Length);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            abort.Cancel(); // разблокируем шарды, припаркованные на WriteAsync(_batches)
            throw;
        }
    }
}

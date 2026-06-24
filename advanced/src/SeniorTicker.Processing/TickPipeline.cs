using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Конвейер обработки в памяти: input → router (stableHash(TickKey) % P) → P шардов
/// (single-writer дедупликация + батч) → общий Channel&lt;Tick[]&gt; → K writer-воркеров → ITickSink.
/// Все каналы ограниченные (bounded, FullMode.Wait): когда канал полон, предыдущая стадия ждёт,
/// поэтому память под контролем. Остановка бывает штатной (Input.Complete, стадии дочитывают
/// остаток и завершаются по очереди) или аварийной (отмена ct).
/// </summary>
public sealed class TickPipeline
{
    private readonly PipelineOptions _opt;
    private readonly ITickSink _sink;
    private readonly IMetricsSink _metrics;
    private readonly TimeProvider _time;
    private readonly IShardPartitioner _partitioner;

    // Вход конвейера: единая очередь сырых тиков. Пишут многие коннекторы, читает один роутер.
    // Backpressure-стадия №1 (источник ↔ роутер): полон → коннектор ждёт → реже читает сокет.
    private readonly Channel<Tick> _ingest;
    // P независимых очередей, по одной на ShardWorker. Роутер раскладывает тик: shard = hash(TickKey) % P.
    // Каждую очередь читает ровно один воркер (single-consumer) — это и есть инвариант дедупликации.
    // Backpressure-стадия №2 (роутер ↔ шард): полон → роутер ждёт на WriteAsync именно этого шарда.
    private readonly Channel<Tick>[] _shards;
    // Общая очередь готовых батчей Tick[]. Пишут P шардов, читают K writer-воркеров.
    // Backpressure-стадия №3 (шарды ↔ запись в sink/БД): полон → шарды ждут → давление катится назад к сокету.
    private readonly Channel<Tick[]> _batches;

    public TickPipeline(
        PipelineOptions options,
        ITickSink sink,
        IMetricsSink metrics,
        TimeProvider time,
        IShardPartitioner? partitioner = null)
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
        _partitioner = partitioner ?? new DedupKeyShardPartitioner();

        // FullMode.Wait = переполнение не дропает тик и не копит в RAM, а тормозит писателя (backpressure).
        _ingest = Channel.CreateBounded<Tick>(new BoundedChannelOptions(options.IngestCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,   // один router читает вход → рантайм берёт оптимизированный single-reader путь
            SingleWriter = false,  // много коннекторов пишут конкурентно → запись под локом
        });

        _shards = new Channel<Tick>[options.ShardCount];
        for (var i = 0; i < _shards.Length; i++)
        {
            // single-reader + single-writer = самый дешёвый режим канала (минимум синхронизации).
            _shards[i] = Channel.CreateBounded<Tick>(new BoundedChannelOptions(options.ShardCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,   // один ShardWorker = один поток-потребитель (инвариант дедупликации)
                SingleWriter = true,   // один router пишет в шард
            });
        }

        // many-reader + many-writer → общий конкурентный режим (под локом): и пишущих, и читающих несколько.
        _batches = Channel.CreateBounded<Tick[]>(new BoundedChannelOptions(options.BatchChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,  // K writers читают
            SingleWriter = false,  // P шардов пишут
        });
    }

    /// <summary>Точка входа для продюсеров (коннекторов). Когда канал полон, продюсеры ждут.</summary>
    public ChannelWriter<Tick> Input => _ingest.Writer;

    /// <summary>Глубина входного канала (метрика): если полон, узкое место в дедупликации или батче.</summary>
    public int IngestDepth => _ingest.Reader.Count;

    /// <summary>Глубина батч-канала (метрика): если полон, узкое место в записи в базу.</summary>
    public int BatchDepth => _batches.Reader.Count;

    public int[] ShardDepths => _shards.Select(s => s.Reader.Count).ToArray();

    public async Task RunAsync(CancellationToken ct)
    {
        // Общий токен отмены для всех воркеров (связан с внешним ct).
        using var abort = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // Шаг 1. Создаём и запускаем P воркеров, по одному на шард. У каждого свой дедупликатор. Воркер
        // читает свой шард-канал _shards[i] и пишет готовые пачки в общий _batches. Ручку задачи кладём в shardTasks.
        var shardTasks = new Task[_shards.Length];
        for (var i = 0; i < _shards.Length; i++)
        {
            var dedup = new SlidingWindowDeduplicator(_opt.DedupWindow, _time);
            var worker = new ShardWorker(dedup, _opt.BatchMaxSize, _opt.BatchMaxDelay, _time, _metrics);
            shardTasks[i] = worker.RunAsync(_shards[i].Reader, _batches.Writer, abort.Token);
        }

        // Шаг 2. Запускаем роутер: он читает _ingest и раскладывает каждый тик в шард по hash(TickKey) % P.
        var routerTask = RouteAsync(abort.Token);

        // Шаг 3. Запускаем фоновую задачу: ждёт завершения всех shardTasks и закрывает писателя _batches.
        var shardsCompletion = CompleteBatchesWhenShardsDoneAsync(shardTasks);

        // Шаг 4. Создаём и запускаем K writer-воркеров: каждый читает _batches и пишет пачки в sink (БД).
        var writerTasks = new Task[_opt.WriterCount];
        for (var i = 0; i < writerTasks.Length; i++)
            writerTasks[i] = WriteLoopAsync(abort.Token, abort);

        // Шаг 5. Собираем все задачи в один список и ждём их завершения: роутер, закрытие батчей, writers.
        var all = new List<Task>(2 + writerTasks.Length) { routerTask, shardsCompletion };
        all.AddRange(writerTasks);
        try
        {
            await Task.WhenAll(all);
        }
        catch
        {
            // Поверх отмены приоритетно поднимаем осмысленную ошибку (например, сбой sink),
            // сохраняя её стек, а не производную OperationCanceledException.
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
        try { await Task.WhenAll(shardTasks); }
        finally { _batches.Writer.TryComplete(); }
    }

    private async Task RouteAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var tick in _ingest.Reader.ReadAllAsync(ct))
            {
                _metrics.OnReceived();
                var shard = _shards[_partitioner.GetShard(tick, _shards.Length)];
                await shard.Writer.WriteAsync(tick, ct); // канал шарда полон, ждём
            }
        }
        finally
        {
            // вход завершён (или отмена) → завершаем все шард-каналы, чтобы воркеры дописали остаток
            foreach (var shard in _shards)
                shard.Writer.TryComplete();
        }
    }

    private async Task WriteLoopAsync(CancellationToken ct, CancellationTokenSource abort)
    {
        try
        {
            await foreach (var batch in _batches.Reader.ReadAllAsync(ct))
            {
                await _sink.WriteBatchAsync(batch, ct);
                _metrics.OnWritten(batch.Length);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            abort.Cancel(); // разблокируем шарды, ждущие на WriteAsync(_batches)
            throw;
        }
    }
}

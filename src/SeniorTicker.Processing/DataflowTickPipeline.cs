using System.Threading.Tasks.Dataflow;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

public sealed class DataflowTickPipeline
{
    private readonly BufferBlock<Tick> _input;
    private readonly ActionBlock<Tick> _router;
    private readonly TransformManyBlock<Tick, Tick>[] _dedupBlocks;
    private readonly BatchBlock<Tick>[] _batchBlocks;
    private readonly ActionBlock<Tick[]> _writer;
    private readonly IShardPartitioner _partitioner;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _completion;

    public DataflowTickPipeline(
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

        _partitioner = partitioner ?? new DedupKeyShardPartitioner();

        _input = new BufferBlock<Tick>(new DataflowBlockOptions
        {
            BoundedCapacity = options.IngestCapacity,
            CancellationToken = _stop.Token,
        });

        _writer = new ActionBlock<Tick[]>(async batch =>
        {
            await sink.WriteBatchAsync(batch, _stop.Token);
            metrics.OnWritten(batch.Length);
        }, new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = options.BatchChannelCapacity,
            MaxDegreeOfParallelism = options.WriterCount,
            EnsureOrdered = false,
            CancellationToken = _stop.Token,
        });

        _dedupBlocks = new TransformManyBlock<Tick, Tick>[options.ShardCount];
        _batchBlocks = new BatchBlock<Tick>[options.ShardCount];
        var flushLoops = new Task[options.ShardCount];

        for (var i = 0; i < options.ShardCount; i++)
        {
            var dedup = new SlidingWindowDeduplicator(options.DedupWindow, time);
            _dedupBlocks[i] = new TransformManyBlock<Tick, Tick>(tick =>
            {
                if (dedup.IsDuplicate(tick))
                {
                    metrics.OnDeduplicated();
                    return [];
                }

                return new[] { tick };
            }, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = options.ShardCapacity,
                MaxDegreeOfParallelism = 1,
                EnsureOrdered = true,
                CancellationToken = _stop.Token,
            });

            _batchBlocks[i] = new BatchBlock<Tick>(options.BatchMaxSize, new GroupingDataflowBlockOptions
            {
                BoundedCapacity = options.ShardCapacity,
                CancellationToken = _stop.Token,
            });

            _dedupBlocks[i].LinkTo(_batchBlocks[i], new DataflowLinkOptions { PropagateCompletion = true });
            _batchBlocks[i].LinkTo(_writer);
            flushLoops[i] = FlushPartialBatchesAsync(_batchBlocks[i], options.BatchMaxDelay, time, _stop.Token);
        }

        _router = new ActionBlock<Tick>(async tick =>
        {
            metrics.OnReceived();
            var shard = _partitioner.GetShard(tick, _dedupBlocks.Length);
            await _dedupBlocks[shard].SendAsync(tick, _stop.Token);
        }, new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = options.IngestCapacity,
            MaxDegreeOfParallelism = 1,
            EnsureOrdered = true,
            CancellationToken = _stop.Token,
        });

        _input.LinkTo(_router, new DataflowLinkOptions { PropagateCompletion = true });
        _completion = CompleteAsync(flushLoops);
    }

    public int IngestDepth => _input.Count;
    public int BatchDepth => _writer.InputCount;
    public int[] ShardDepths => _dedupBlocks.Select((b, i) => b.InputCount + _batchBlocks[i].OutputCount).ToArray();
    public Task Completion => _completion;

    public Task<bool> SendAsync(Tick tick, CancellationToken ct = default)
        => _input.SendAsync(tick, ct);

    public void Complete() => _input.Complete();

    public void Abort() => _stop.Cancel();

    private async Task CompleteAsync(Task[] flushLoops)
    {
        try
        {
            await _router.Completion;
        }
        finally
        {
            foreach (var block in _dedupBlocks)
                block.Complete();
        }

        await Task.WhenAll(_dedupBlocks.Select(b => b.Completion));
        await Task.WhenAll(_batchBlocks.Select(b => b.Completion));
        _writer.Complete();
        await _writer.Completion;

        _stop.Cancel();
        await Task.WhenAll(flushLoops);
    }

    private static async Task FlushPartialBatchesAsync(
        BatchBlock<Tick> block,
        TimeSpan delay,
        TimeProvider time,
        CancellationToken ct)
    {
        try
        {
            while (true)
            {
                await Task.Delay(delay, time, ct);
                block.TriggerBatch();
            }
        }
        catch (OperationCanceledException)
        {
            block.TriggerBatch();
        }
    }
}

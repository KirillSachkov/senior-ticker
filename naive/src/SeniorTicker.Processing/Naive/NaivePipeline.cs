using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант» конвейера целиком, как <see cref="ITickPipeline"/> — чтобы Host запускал его по
/// тумблеру <c>Pipeline:Mode=Naive</c> и провал был виден вживую. Антипример к <see cref="TickPipeline"/>:
/// (1) ОДИН общий <see cref="NaiveDeduplicator"/> на все обработчики — гонка и коллизия ключа;
/// (2) запись по одному тику из многих потоков — без батчей и без одного владельца;
/// (3) безлимитный вход — нет backpressure, при медленной БД память растёт.
/// Штатную остановку обрабатывает Host (Input.Complete + дренаж); наивную остановку одним токеном
/// см. <see cref="NaiveBufferingProcessor"/> и тест SimpleVariantTests.
/// </summary>
public sealed class NaivePipeline(ITickSink sink, IMetricsSink metrics) : ITickPipeline
{
    private readonly NaiveDeduplicator _dedup = new();
    private readonly Channel<Tick> _ingest = Channel.CreateUnbounded<Tick>();

    public ChannelWriter<Tick> Input => _ingest.Writer;
    public int IngestDepth => _ingest.Reader.Count;
    public int BatchDepth => 0;
    public int[] ShardDepths => [];

    public async Task RunAsync(CancellationToken ct)
    {
        var workers = Enumerable.Range(0, Math.Max(2, Environment.ProcessorCount / 2))
            .Select(_ => Task.Run(() => WorkAsync(ct), ct))
            .ToArray();
        await Task.WhenAll(workers);
    }

    private async Task WorkAsync(CancellationToken ct)
    {
        await foreach (var tick in _ingest.Reader.ReadAllAsync(ct))
        {
            metrics.OnReceived();
            if (_dedup.IsDuplicate(tick))
            {
                metrics.OnDeduplicated();
                continue;
            }

            await sink.WriteBatchAsync(new[] { tick }, ct);
            metrics.OnWritten(1);
        }
    }
}

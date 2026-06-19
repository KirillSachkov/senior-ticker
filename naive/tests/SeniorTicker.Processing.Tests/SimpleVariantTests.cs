using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Processing.Naive;
using Xunit;

namespace SeniorTicker.Processing.Tests;

/// <summary>
/// КРАСНЫЕ ПО ЗАМЫСЛУ. Прогоняют «простой вариант» (Naive*) через те же инварианты, что держит
/// боевой конвейер, и показывают, что он их НЕ держит: коллизия/гонка общего дедупа, потеря данных
/// при записи из многих потоков, потеря буфера при остановке одним токеном. Красный тест здесь —
/// это урок: почему дедуп нуждается в одном владельце, запись — в батчах и соединении-на-воркер,
/// а остановка — в двухфазном дренаже. Зелёный контраст — в TickPipelineTests и TenProblemsRegressionTests.
/// (CI нет, это учебный проект — тесты остаются красными намеренно.)
/// </summary>
[Trait("Category", "SimpleVariantDemo")]
public class SimpleVariantTests
{
    [Fact]
    public void Dedup_collapses_distinct_ticks_in_same_millisecond()
    {
        // Инвариант: два события одного символа в одну миллисекунду с разными SourceId — РАЗНЫЕ.
        var dedup = new NaiveDeduplicator();
        var a = TickFactory.New("BTCUSDT", sourceId: 1);
        var b = TickFactory.New("BTCUSDT", sourceId: 2); // тот же symbol+timestamp, другой SourceId

        Assert.False(dedup.IsDuplicate(a)); // первый — новый
        Assert.False(dedup.IsDuplicate(b)); // КРАСНОЕ: наивный ключ теряет SourceId → схлопывает в дубль
    }

    [Fact]
    public async Task Dedup_under_concurrent_producers_loses_unique_ticks()
    {
        // Инвариант: 1000 уникальных ключей → 1000 записей (ср. High_parallel_producer_load в TickPipelineTests).
        var sink = new InMemoryTickSink();
        var processor = new NaiveTickIngestor(new NaiveDeduplicator(), sink, new CountingMetricsSink()); // ОДИН общий дедуп

        const int producers = 16, perProducer = 500, uniqueKeys = 1000;
        var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
            {
                var id = (p * perProducer + i) % uniqueKeys;
                await processor.IngestAsync(TickFactory.New($"S{id % 20}", id), default);
            }
        })).ToArray();
        await Task.WhenAll(tasks);

        // КРАСНОЕ: ключ игнорирует SourceId, а общий дедуп гоняется потоками → записей далеко не 1000.
        Assert.Equal(uniqueKeys, sink.All.Count);
        Assert.Equal(uniqueKeys, sink.All.Select(t => t.Key).Distinct().Count());
    }

    [Fact]
    public async Task Direct_writes_from_many_handlers_lose_data()
    {
        // Инвариант: ни один принятый тик не теряется (боевой sink — connection-per-writer, потокобезопасен).
        var sink = new NaiveSharedListSink(); // общий НЕ-потокобезопасный List, как один общий DbContext
        var processor = new NaiveTickIngestor(new NoopDeduplicator(), sink, new CountingMetricsSink());

        const int producers = 16, perProducer = 500, total = producers * perProducer;
        var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
                await processor.IngestAsync(TickFactory.New($"S{p}", p * perProducer + i), default);
        })).ToArray();
        await Task.WhenAll(tasks);

        // КРАСНОЕ: параллельная запись в один общий List теряет/портит данные (или бросает).
        Assert.Equal(total, sink.Count);
    }

    [Fact]
    public async Task Single_cancellation_loses_buffer_on_stop()
    {
        // Инвариант: принятые до остановки тики доезжают до sink (ср. Problem05_shutdown_drains_buffer).
        var sink = new InMemoryTickSink();
        var processor = new NaiveBufferingProcessor(sink);
        using var cts = new CancellationTokenSource();

        var run = processor.RunAsync(cts.Token);
        for (var i = 0; i < 100; i++)
            await processor.Input.WriteAsync(TickFactory.New("BTC", i));

        await cts.CancelAsync();        // штатная остановка одним общим токеном
        await run;

        // КРАСНОЕ: один токен оборвал цикл чтения, buffer не дописан → всё потеряно.
        Assert.Equal(100, sink.All.Count);
    }

    // --- наивные тест-дойблы ---

    /// <summary>Дедуп-заглушка: ничего не отсеивает — чтобы изолировать провал ЗАПИСИ.</summary>
    private sealed class NoopDeduplicator : IDeduplicator
    {
        public bool IsDuplicate(in Tick tick) => false;
    }

    /// <summary>Модель одного общего DbContext: повторный параллельный вход недопустим и бросает,
    /// как EF («A second operation was started…»). Делает провал детерминированным, без флака.</summary>
    private sealed class NaiveSharedListSink : ITickSink
    {
        private readonly List<Tick> _all = [];
        private int _inUse;
        public int Count => _all.Count;

        public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _inUse, 1) == 1)
                throw new InvalidOperationException(
                    "A second operation was started on this context before a previous operation completed.");
            try
            {
                for (var i = 0; i < batch.Length; i++)
                {
                    _all.Add(batch.Span[i]);
                    await Task.Delay(1, ct); // окно, чтобы параллельный вход поймал занятость
                }
            }
            finally
            {
                _inUse = 0;
            }
        }
    }
}

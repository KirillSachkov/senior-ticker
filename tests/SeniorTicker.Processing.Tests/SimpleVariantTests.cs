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
        var processor = new NaiveTickProcessor(new NaiveDeduplicator(), sink); // ОДИН общий дедуп

        const int producers = 16, perProducer = 500, uniqueKeys = 1000;
        var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
            {
                var id = (p * perProducer + i) % uniqueKeys;
                await processor.HandleAsync(TickFactory.New($"S{id % 20}", id));
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
        var processor = new NaiveTickProcessor(new NoopDeduplicator(), sink);

        const int producers = 16, perProducer = 500, total = producers * perProducer;
        var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
                await processor.HandleAsync(TickFactory.New($"S{p}", p * perProducer + i));
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

    /// <summary>Общий НЕ-потокобезопасный список — модель одного общего DbContext/соединения.</summary>
    private sealed class NaiveSharedListSink : ITickSink
    {
        private readonly List<Tick> _all = [];
        public int Count => _all.Count;

        public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        {
            foreach (var t in batch.Span)
                _all.Add(t); // НЕ потокобезопасно: гонка на внутреннем массиве/счётчике
            return Task.CompletedTask;
        }
    }
}

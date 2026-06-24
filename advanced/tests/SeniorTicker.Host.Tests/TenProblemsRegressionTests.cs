using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Polly;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Host.Observability;
using SeniorTicker.Infrastructure.WebSockets;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Tests;

/// <summary>
/// ЯВНЫЙ proof-артефакт: по одному тесту на каждую из 10 проблем эталонного код-ревью ученика (§15).
/// Доказательство, что эталон закрыл их by design. Восемь юнит-уровневых здесь; #4 (connection-per-writer)
/// и #10 (миграции через IDbContextFactory) доказаны против реального Postgres в SeniorTicker.Persistence.Tests
/// (Concurrent_writers_each_own_connection_no_corruption, Initialize_applies_migrations_via_factory_and_is_idempotent).
/// Полная карта — в README §«10 проблем закрыты».
/// </summary>
public class TenProblemsRegressionTests
{
    private static Tick Tick(long sourceId, string symbol = "BTCUSDT", DateTimeOffset? ts = null)
    {
        var t = ts ?? new DateTimeOffset(2026, 5, 30, 12, 0, 0, TimeSpan.Zero);
        return new Tick(Exchange.Binance, symbol, 50000m, 0.5m, t, sourceId, t);
    }

    // ── #1: гонка в дедупликаторе — single-writer делает её невозможной ПО КОНСТРУКЦИИ ──────────────
    [Fact]
    public async Task Problem01_concurrent_producers_cannot_race_the_deduplicator()
    {
        const int unique = 5000;
        const int producers = 8;
        var sink = new CountingTickSink();
        var metrics = new CountingMetricsSink();
        var pipeline = new TickPipeline(RegressionOptions(), sink, metrics, TimeProvider.System);
        var run = pipeline.RunAsync(CancellationToken.None);

        // 8 продюсеров льют ОДИН и тот же набор ключей одновременно → 8×unique тиков, unique уникальных.
        await Task.WhenAll(Enumerable.Range(0, producers).Select(_ => Task.Run(async () =>
        {
            for (long i = 0; i < unique; i++)
                await pipeline.Input.WriteAsync(Tick(i));
        })));
        pipeline.Input.Complete();
        await run;

        // РОВНО unique записано, остальное — дубли. Гонка (TryAdd+Exchange) дала бы недетерминированный счёт.
        Assert.Equal(unique, sink.Written);
        Assert.Equal((long)(producers - 1) * unique, metrics.Deduplicated);
    }

    // ── #2: 32-битный хеш-ключ → ложные схлопывания. Точный составной ключ с тайбрейкером ───────────
    [Fact]
    public void Problem02_distinct_ticks_in_same_millisecond_are_not_collapsed()
    {
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), TimeProvider.System);
        var ts = new DateTimeOffset(2026, 5, 30, 12, 0, 0, TimeSpan.Zero);

        var a = Tick(sourceId: 1, ts: ts);
        var b = Tick(sourceId: 2, ts: ts);   // тот же символ и та же миллисекунда, но другой SourceId

        Assert.False(dedup.IsDuplicate(a));  // первый — новый
        Assert.False(dedup.IsDuplicate(b));  // ВТОРОЙ ТОЖЕ новый — не схлопнут (точный ключ, не хеш)
        Assert.True(dedup.IsDuplicate(a));   // повтор того же ключа — дубль
    }

    // ── #3: переполнение int % → отрицательный индекс шарда. Маска в неотрицательное по построению ──
    [Fact]
    public void Problem03_shard_index_is_always_in_range_for_adversarial_symbols()
    {
        const int shards = 7;
        // широкий набор символов, включая такие, что при int-хеше с оператором % дали бы отрицательный индекс.
        for (var i = 0; i < 20_000; i++)
        {
            var symbol = $"SYM{i}/USDT-{(char)('A' + (i % 26))}{i * 2654435761u}";
            var shard = StableHash.ShardOf(symbol, shards);
            Assert.InRange(shard, 0, shards - 1);  // НИКОГДА не отрицательный, всегда [0,P)
        }
    }

    // ── #4: общий DbContext/captive dependency → connection-per-writer. Доказано в Persistence.Tests ─
    // см. Concurrent_writers_each_own_connection_no_corruption (Testcontainers, реальный Postgres).

    // ── #5: shutdown теряет данные → двухфазный дренаж дописывает буфер ─────────────────────────────
    [Fact]
    public async Task Problem05_shutdown_drains_buffer_without_loss()
    {
        const int n = 3000;
        var sink = new CountingTickSink();
        var pipeline = new TickPipeline(RegressionOptions(), sink, new MetricsSink(), TimeProvider.System);
        var run = pipeline.RunAsync(CancellationToken.None);

        for (long i = 0; i < n; i++)
            await pipeline.Input.WriteAsync(Tick(i));
        pipeline.Input.Complete();   // фаза дренажа: НЕ отмена → ничего не теряется
        await run;

        Assert.Equal(n, sink.Written);
    }

    // ── #6: Polly ловит OperationCanceledException → бесконечный retry на shutdown. v8 исключает OCE ─
    // ВАЖНО: токен НЕ предотменяем (иначе Polly прервал бы любой retry по состоянию токена, и тест был бы
    // зелёным даже при реверте фикса). Различаем именно по ShouldHandle: OCE НЕ ретраится, TimeoutException —
    // ретраится. Делеи крошечные → реверт на catch-all ретраил бы бесконечно → WaitAsync поймал бы зависание.
    [Fact]
    public async Task Problem06_policy_excludes_OCE_but_retries_other_faults()
    {
        var fast = new WebSocketConnectorOptions
        {
            Name = "x",
            Url = new Uri("wss://x/ws"),
            ReconnectBaseDelay = TimeSpan.FromMilliseconds(1),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(5),
        };

        // (1) НЕГАТИВ: bare OCE на НЕ-отменённом токене → дефолтный ShouldHandle исключает OCE → НЕ ретраит,
        //     всплывает сразу (catch-all-реверт ретраил бы вечно → WaitAsync → TimeoutException → падение).
        var oceRetries = 0;
        var ocePipeline = ResiliencePipelineFactory.CreateReconnectPipeline(fast, (_, _, _) => oceRetries++);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await ocePipeline.ExecuteAsync(static _ => throw new OperationCanceledException(), CancellationToken.None)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(0, oceRetries);

        // (2) ПОЗИТИВ: TimeoutException (idle/connect-таймаут) — НЕ OCE → РЕТРАИТСЯ. Останавливаем после
        //     первого retry, отменяя токен из onRetry → доказывает, что различение по ShouldHandle, не по токену.
        using var stop = new CancellationTokenSource();
        var toRetries = 0;
        var toPipeline = ResiliencePipelineFactory.CreateReconnectPipeline(fast, (_, _, _) => { toRetries++; stop.Cancel(); });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await toPipeline.ExecuteAsync(static _ => throw new TimeoutException(), stop.Token));
        Assert.True(toRetries >= 1);
    }

    // ── #7: рассинхрон метрик (частичный reset). Стандартный аддитивный Counter снимает проблему ─────
    [Fact]
    public void Problem07_metrics_go_through_standard_additive_counters()
    {
        using var metrics = new MetricsSink();
        using var received = new MetricCollector<long>(metrics.Meter, "ticker.received");
        using var deduplicated = new MetricCollector<long>(metrics.Meter, "ticker.deduplicated");
        using var written = new MetricCollector<long>(metrics.Meter, "ticker.written");

        metrics.OnReceived(10);
        metrics.OnDeduplicated(3);
        metrics.OnWritten(7);

        // Counter<long> монотонен и аддитивен по конструкции: ручного snapshot/reset (баг ученика,
        // рассинхронивший метрики) тут нет. gap = received − written считает потребитель, не хранится.
        Assert.Equal(10, received.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(3, deduplicated.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(7, written.GetMeasurementSnapshot().Sum(m => m.Value));
        Assert.Equal(3,
            received.GetMeasurementSnapshot().Sum(m => m.Value) - written.GetMeasurementSnapshot().Sum(m => m.Value));
    }

    // ── #8: фикс-буфер 16 КБ → потеря крупных кадров. Растущий буфер до cap + abort за cap ───────────
    [Fact]
    public async Task Problem08_receiver_grows_past_initial_buffer_and_caps_at_max()
    {
        // сообщение 100 байт при стартовом буфере 16 → собирается полностью (рост)
        var receiver = new WebSocketMessageReceiver(initialBufferBytes: 16, maxMessageBytes: 1024);
        var payload = Encoding.UTF8.GetBytes(new string('x', 100));
        using (var msg = await receiver.ReceiveAsync(new FakeWebSocket([(payload, true, false)]), CancellationToken.None))
        {
            Assert.False(msg.IsClosed);
            Assert.Equal(100, msg.Span.Length);   // переросло начальный буфер, собрано без потери
        }

        // сообщение за cap → abort (а не молчаливый приём усечённого / OOM)
        var capped = new WebSocketMessageReceiver(initialBufferBytes: 16, maxMessageBytes: 50);
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            using var _ = await capped.ReceiveAsync(new FakeWebSocket([(new byte[100], true, false)]), CancellationToken.None);
        });
    }

    // ── #9: debug-коннектор в проде → enabled-only + wss-гейт на старте (fail boot) ─────────────────
    [Fact]
    public void Problem09_public_ws_connector_fails_boot()
    {
        var validator = new SeniorTickerOptionsValidator();
        var bad = new SeniorTickerOptions
        {
            Exchanges = [new ExchangeConfig { Name = "debug", Format = ExchangeFormat.Binance, Url = "ws://public.example.com/ws" }],
        };
        Assert.True(validator.Validate(null, bad).Failed);   // публичный ws → хост не стартует

        var good = new SeniorTickerOptions
        {
            Exchanges = [new ExchangeConfig { Name = "prod", Format = ExchangeFormat.Binance, Url = "wss://stream.binance.com/ws" }],
        };
        Assert.True(validator.Validate(null, good).Succeeded);
    }

    // ── #10: sync EnsureCreated() в конструкторе → миграции IHostedService через IDbContextFactory ───
    // см. Initialize_applies_migrations_via_factory_and_is_idempotent (Persistence.Tests, Testcontainers).

    // ── Перф-инвариант §7.2: production-дефолт батча держит массив ниже LOH ──────────────────────────
    [Fact]
    public void Batch_default_keeps_array_below_LOH_threshold()
    {
        var batchBytes = new PipelineConfig().BatchMaxSize * Unsafe.SizeOf<Tick>();
        Assert.True(batchBytes < 85_000,
            $"BatchMaxSize default × sizeof(Tick)={Unsafe.SizeOf<Tick>()} = {batchBytes}B must stay below LOH 85_000");
    }

    private static PipelineOptions RegressionOptions() => new()
    {
        ShardCount = 1,
        WriterCount = 2,
        IngestCapacity = 4096,
        ShardCapacity = 4096,
        BatchChannelCapacity = 16,
        BatchMaxSize = 900,
        BatchMaxDelay = TimeSpan.FromMilliseconds(50),
        DedupWindow = TimeSpan.FromMinutes(5),
    };

    private sealed class CountingTickSink : ITickSink
    {
        private long _written;
        public long Written => Interlocked.Read(ref _written);
        public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
        {
            Interlocked.Add(ref _written, batch.Length);
            return Task.CompletedTask;
        }
    }

    // Дубль порта метрик: читаем счётчики напрямую, не завязываясь на механизм экспорта (тест про
    // конвейер, а не про OTel).
    private sealed class CountingMetricsSink : IMetricsSink
    {
        private long _deduplicated;
        public long Deduplicated => Interlocked.Read(ref _deduplicated);
        public void OnReceived(long n = 1) { }
        public void OnDeduplicated(long n = 1) => Interlocked.Add(ref _deduplicated, n);
        public void OnWritten(long n) { }
        public void OnDropped(long n = 1) { }
    }

    // WebSocket-дублёр: отдаёт заданные фрагменты, режа по размеру буфера (для теста растущего буфера).
    private sealed class FakeWebSocket(IEnumerable<(byte[] Data, bool EndOfMessage, bool Close)> frames) : WebSocket
    {
        private readonly LinkedList<(byte[] Data, bool EndOfMessage, bool Close)> _frames = new(frames);

        public override ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken ct)
        {
            if (_frames.Count == 0)
                return ValueTask.FromResult(new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true));
            var (data, eom, close) = _frames.First!.Value;
            _frames.RemoveFirst();
            if (close)
                return ValueTask.FromResult(new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true));
            var n = Math.Min(data.Length, buffer.Length);
            data.AsSpan(0, n).CopyTo(buffer.Span);
            if (n < data.Length)
                _frames.AddFirst((data[n..], eom, false));
            return ValueTask.FromResult(new ValueWebSocketReceiveResult(n, WebSocketMessageType.Binary, n == data.Length && eom));
        }

        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override WebSocketState State => WebSocketState.Open;
        public override string? SubProtocol => null;
        public override void Abort() { }
        public override Task CloseAsync(WebSocketCloseStatus s, string? d, CancellationToken ct) => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus s, string? d, CancellationToken ct) => Task.CompletedTask;
        public override void Dispose() { }
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken ct)
            => throw new NotSupportedException();
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType t, bool eom, CancellationToken ct)
            => Task.CompletedTask;
    }
}

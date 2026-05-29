# SeniorTicker — План 2: Ingestion (MockExchange + WebSockets-коннекторы)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Реализовать подсистему сбора: коннекторы к 2–3 биржам по WebSocket с разными форматами сообщений, нормализацию + валидацию, автопереподключение, и mock-биржу (WS-сервер-заглушку) для честного тестирования lifecycle и reconnect.

**Architecture:** Новые порты в `Application` (`ITickIngestor`, `IMessageParser`, `IExchangeConnector`). Новый проект `SeniorTicker.Infrastructure.WebSockets`: `WebSocketMessageReceiver` (растущий ArrayPool-буфер + `MaxMessageBytes` → фикс #8), per-exchange парсеры (Binance JSON / Kraken array / CSV), `TickValidator` (гейт значений), `WebSocketConnectorBase` (reconnect через Polly v8 → фикс #6, лог connect/disconnect/error). Новый проект `SeniorTicker.MockExchange` (ASP.NET Core, 3 WS-эндпоинта с разными форматами). Коннекторы пушат `Tick` в `ITickIngestor` (producer-seam; Host свяжет его с `TickPipeline.Input` в Плане 4).

**Tech Stack:** .NET 10, `System.Net.WebSockets.ClientWebSocket`, ASP.NET Core WebSockets, `System.Text.Json` source-gen + `Utf8JsonReader`, Polly v8 `ResiliencePipeline`, xUnit + интеграционные тесты против живого Kestrel.

**Опора:** spec `docs/superpowers/specs/2026-05-29-senior-ticker-design.md` (§3 декомпозиция, §5.1 async, §11 безопасность/валидация, §15 №6/№8/№9). Плана 1 артефакты уже в `main` (Domain/Application/Processing, 24 теста).

**Producer-seam:** коннекторы зависят от `ITickIngestor` (Application), НЕ от `Processing`. В Плане 4 Host реализует `ITickIngestor` поверх `TickPipeline.Input`. В тестах — `CollectingIngestor`.

---

## File Structure (создаётся этим планом)

```
src/
├─ SeniorTicker.Application/
│  ├─ ITickIngestor.cs                # producer-seam: коннектор → конвейер
│  ├─ IMessageParser.cs              # парсер одного формата: UTF-8 кадр → Tick
│  └─ IExchangeConnector.cs          # одна запущенная WS-биржа (connect/receive/reconnect)
├─ SeniorTicker.Infrastructure.WebSockets/
│  ├─ SeniorTicker.Infrastructure.WebSockets.csproj   # ref: Application, Domain; pkg: Polly
│  ├─ WebSocketConnectorOptions.cs
│  ├─ TickValidator.cs               # гейт: цена>0, объём>=0, окно времени, символ
│  ├─ WebSocketMessageReceiver.cs    # растущий буфер до MaxMessageBytes (фикс #8)
│  ├─ ReceivedMessage.cs             # readonly struct: rented buffer + Dispose→возврат в пул
│  ├─ ResiliencePipelineFactory.cs   # Polly v8 reconnect (retry-forever + backoff+jitter)
│  ├─ WebSocketConnectorBase.cs      # базовый коннектор: lifecycle + reconnect + лог
│  ├─ ExchangeMessageJsonContext.cs  # STJ source-gen контекст для DTO
│  └─ Parsers/
│     ├─ BinanceMessageParser.cs     # {"s","p","q","T","a"}
│     ├─ KrakenMessageParser.cs      # [chanId,[price,vol,time],"trade","BTC/USD"]
│     └─ CsvMessageParser.cs         # symbol,price,volume,epochMs,id
└─ SeniorTicker.MockExchange/
   ├─ SeniorTicker.MockExchange.csproj  # Microsoft.NET.Sdk.Web
   ├─ Program.cs                      # 3 WS-эндпоинта, таймерная генерация, drop-после-N
   └─ MockTickGenerator.cs            # детерминированная генерация + 3 сериализатора формата
tests/
├─ SeniorTicker.Processing.Tests/  (существует — НЕ трогаем)
├─ SeniorTicker.WebSockets.Tests/     # юнит: парсеры, валидатор, receiver (+FakeWebSocket)
│  ├─ ... .csproj                     # ref: Infrastructure.WebSockets
│  ├─ TestDoubles.cs                  # CollectingIngestor, FakeWebSocket
│  ├─ BinanceMessageParserTests.cs
│  ├─ KrakenMessageParserTests.cs
│  ├─ CsvMessageParserTests.cs
│  ├─ TickValidatorTests.cs
│  └─ WebSocketMessageReceiverTests.cs
└─ SeniorTicker.Integration.Tests/    # интеграция: коннектор ↔ живой MockExchange
   ├─ ... .csproj                     # ref: WebSockets, MockExchange; pkg: Microsoft.AspNetCore.Mvc.Testing
   ├─ ConnectorReceivesAndNormalizesTests.cs
   └─ ConnectorReconnectTests.cs
```

---

### Task 1: Application — новые порты

**Files:** Create `src/SeniorTicker.Application/ITickIngestor.cs`, `IMessageParser.cs`, `IExchangeConnector.cs`

- [ ] **Step 1: `ITickIngestor.cs`**

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Producer-seam: коннекторы пушат нормализованные тики сюда. Host связывает реализацию
/// с входом конвейера (TickPipeline.Input). Держит Infrastructure.WebSockets независимым от Processing.
/// </summary>
public interface ITickIngestor
{
    ValueTask IngestAsync(Tick tick, CancellationToken ct);
}
```

- [ ] **Step 2: `IMessageParser.cs`**

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Парсит один формат биржи (UTF-8 кадр) в нормализованный Tick. Одна реализация на формат
/// (расширяемость: новая биржа = новый парсер + коннектор). Допущение: один тик на кадр.
/// </summary>
public interface IMessageParser
{
    /// <returns>true + tick при успешном разборе; false при невалидном/нераспознанном кадре.</returns>
    bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick);
}
```

- [ ] **Step 3: `IExchangeConnector.cs`**

```csharp
namespace SeniorTicker.Application;

/// <summary>
/// Одна запущенная WS-биржа: connect → receive → parse → validate → ingest, с автопереподключением.
/// Работает до отмены ct. SRP: один коннектор на источник; падение одного не валит другие.
/// </summary>
public interface IExchangeConnector
{
    string Name { get; }
    Task RunAsync(CancellationToken ct);
}
```

- [ ] **Step 4: Сборка + commit**

Run: `dotnet build src/SeniorTicker.Application/SeniorTicker.Application.csproj`
Expected: 0 warnings.

```bash
git add src/SeniorTicker.Application
git commit -m "feat(application): ingestion ports ITickIngestor, IMessageParser, IExchangeConnector"
```

---

### Task 2: Проект WebSockets + тест-проект + JSON source-gen + `BinanceMessageParser` (TDD)

**Files:** Create `src/SeniorTicker.Infrastructure.WebSockets/*` (csproj, `ExchangeMessageJsonContext.cs`, `Parsers/BinanceMessageParser.cs`), `tests/SeniorTicker.WebSockets.Tests/*` (csproj, `TestDoubles.cs`, `BinanceMessageParserTests.cs`)

- [ ] **Step 1: Создать проекты + ссылки + пакеты**

```bash
dotnet new classlib -n SeniorTicker.Infrastructure.WebSockets -o src/SeniorTicker.Infrastructure.WebSockets
rm src/SeniorTicker.Infrastructure.WebSockets/Class1.cs
dotnet sln add src/SeniorTicker.Infrastructure.WebSockets/SeniorTicker.Infrastructure.WebSockets.csproj
dotnet add src/SeniorTicker.Infrastructure.WebSockets/SeniorTicker.Infrastructure.WebSockets.csproj reference src/SeniorTicker.Application/SeniorTicker.Application.csproj src/SeniorTicker.Domain/SeniorTicker.Domain.csproj

dotnet new xunit -n SeniorTicker.WebSockets.Tests -o tests/SeniorTicker.WebSockets.Tests
rm tests/SeniorTicker.WebSockets.Tests/UnitTest1.cs
dotnet sln add tests/SeniorTicker.WebSockets.Tests/SeniorTicker.WebSockets.Tests.csproj
dotnet add tests/SeniorTicker.WebSockets.Tests/SeniorTicker.WebSockets.Tests.csproj reference src/SeniorTicker.Infrastructure.WebSockets/SeniorTicker.Infrastructure.WebSockets.csproj
dotnet add tests/SeniorTicker.WebSockets.Tests/SeniorTicker.WebSockets.Tests.csproj package Microsoft.Extensions.TimeProvider.Testing
```

- [ ] **Step 2: Добавить Polly в `Directory.Packages.props`** (если ещё нет): добавить `<PackageVersion Include="Polly.Core" Version="8.4.2" />` в `Directory.Packages.props`, затем сослаться из WebSockets-проекта:

```bash
dotnet add src/SeniorTicker.Infrastructure.WebSockets/SeniorTicker.Infrastructure.WebSockets.csproj package Polly.Core
```
(Если версия не найдена — взять актуальную: `dotnet package search Polly.Core --take 3`, обновить версию в `Directory.Packages.props`.)

- [ ] **Step 3: `ExchangeMessageJsonContext.cs` + Binance DTO (source-gen STJ — быстро, AOT/trim-friendly)**

```csharp
using System.Text.Json.Serialization;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>DTO формата Binance aggTrade-подобного: s=symbol, p=price, q=qty, T=trade time ms, a=agg id.</summary>
public sealed class BinanceTradeDto
{
    [JsonPropertyName("s")] public string? Symbol { get; set; }
    [JsonPropertyName("p")] public string? Price { get; set; }
    [JsonPropertyName("q")] public string? Quantity { get; set; }
    [JsonPropertyName("T")] public long EventTimeMs { get; set; }
    [JsonPropertyName("a")] public long AggTradeId { get; set; }
}

[JsonSourceGenerationOptions(
    PropertyNameCaseInsensitive = false,
    AllowOutOfOrderMetadataProperties = false)]
[JsonSerializable(typeof(BinanceTradeDto))]
public partial class ExchangeMessageJsonContext : JsonSerializerContext;
```

- [ ] **Step 4: `TestDoubles.cs` (тестовые дублёры для WebSockets-тестов)**

```csharp
using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.WebSockets.Tests;

public sealed class CollectingIngestor : ITickIngestor
{
    private readonly ConcurrentQueue<Tick> _ticks = new();
    public ValueTask IngestAsync(Tick tick, CancellationToken ct)
    {
        _ticks.Enqueue(tick);
        return ValueTask.CompletedTask;
    }
    public IReadOnlyCollection<Tick> Ticks => _ticks.ToArray();
}
```

- [ ] **Step 5: Написать падающие тесты `BinanceMessageParserTests.cs`**

```csharp
using System.Text;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class BinanceMessageParserTests
{
    private static ReadOnlySpan<byte> Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static readonly DateTimeOffset Ingest = DateTimeOffset.UnixEpoch;

    [Fact]
    public void Parses_valid_binance_trade()
    {
        var parser = new BinanceMessageParser();
        var json = """{"s":"BTCUSDT","p":"50000.50","q":"0.25","T":1700000000000,"a":98765}""";
        Assert.True(parser.TryParse(Utf8(json), Ingest, out var tick));
        Assert.Equal(Exchange.Binance, tick.Exchange);
        Assert.Equal("BTCUSDT", tick.Symbol);
        Assert.Equal(50000.50m, tick.Price);
        Assert.Equal(0.25m, tick.Volume);
        Assert.Equal(98765, tick.SourceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), tick.ExchangeTimestamp);
        Assert.Equal(Ingest, tick.IngestTimestamp);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("""{"s":"BTCUSDT","p":"notanumber","q":"1","T":1,"a":1}""")]
    [InlineData("""{"s":null,"p":"1","q":"1","T":1,"a":1}""")]
    public void Rejects_malformed_or_incomplete(string raw)
    {
        var parser = new BinanceMessageParser();
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }
}
```

- [ ] **Step 6: Запустить → FAIL** (`BinanceMessageParser` не определён).
Run: `dotnet test tests/SeniorTicker.WebSockets.Tests/SeniorTicker.WebSockets.Tests.csproj`
Expected: FAIL.

- [ ] **Step 7: `Parsers/BinanceMessageParser.cs`**

```csharp
using System.Globalization;
using System.Text.Json;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>Парсер Binance aggTrade-подобного JSON (source-gen STJ). Любой сбой → false (skip+count выше).</summary>
public sealed class BinanceMessageParser : IMessageParser
{
    public bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick)
    {
        tick = default;
        BinanceTradeDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize(utf8Frame, ExchangeMessageJsonContext.Default.BinanceTradeDto);
        }
        catch (JsonException)
        {
            return false;
        }

        if (dto is null || string.IsNullOrEmpty(dto.Symbol)
            || !decimal.TryParse(dto.Price, NumberStyles.Number, CultureInfo.InvariantCulture, out var price)
            || !decimal.TryParse(dto.Quantity, NumberStyles.Number, CultureInfo.InvariantCulture, out var volume))
        {
            return false;
        }

        tick = new Tick(
            Exchange.Binance, dto.Symbol, price, volume,
            DateTimeOffset.FromUnixTimeMilliseconds(dto.EventTimeMs),
            dto.AggTradeId, ingestTimestamp);
        return true;
    }
}
```

- [ ] **Step 8: Запустить → PASS**, 0 warnings. Commit.

```bash
git add src/SeniorTicker.Infrastructure.WebSockets tests/SeniorTicker.WebSockets.Tests Directory.Packages.props
git commit -m "feat(ws): BinanceMessageParser (source-gen STJ) + WebSockets project scaffold"
```

---

### Task 3: `KrakenMessageParser` (TDD) — формат-массив

**Files:** Create `src/SeniorTicker.Infrastructure.WebSockets/Parsers/KrakenMessageParser.cs`, `tests/SeniorTicker.WebSockets.Tests/KrakenMessageParserTests.cs`

- [ ] **Step 1: Падающие тесты `KrakenMessageParserTests.cs`**

```csharp
using System.Text;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class KrakenMessageParserTests
{
    private static ReadOnlySpan<byte> Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static readonly DateTimeOffset Ingest = DateTimeOffset.UnixEpoch;

    [Fact]
    public void Parses_valid_kraken_trade()
    {
        // [channelId, [price, volume, time_seconds], "trade", "BTC/USD", tradeId]
        var parser = new KrakenMessageParser();
        var raw = """[42,["50000.5","0.25","1700000000.500"],"trade","BTC/USD",12345]""";
        Assert.True(parser.TryParse(Utf8(raw), Ingest, out var tick));
        Assert.Equal(Exchange.Kraken, tick.Exchange);
        Assert.Equal("BTC/USD", tick.Symbol);
        Assert.Equal(50000.5m, tick.Price);
        Assert.Equal(0.25m, tick.Volume);
        Assert.Equal(12345, tick.SourceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000500), tick.ExchangeTimestamp);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"not":"array"}""")]
    [InlineData("""[42,["x","0.25","1700000000.5"],"trade","BTC/USD",1]""")]
    [InlineData("""[42,["50000"],"trade","BTC/USD",1]""")]
    public void Rejects_malformed(string raw)
    {
        var parser = new KrakenMessageParser();
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }
}
```

- [ ] **Step 2: Запустить → FAIL.**

- [ ] **Step 3: `Parsers/KrakenMessageParser.cs`** (ручной разбор массива через `Utf8JsonReader` — формат не маппится на DTO красиво)

```csharp
using System.Buffers;
using System.Globalization;
using System.Text.Json;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>
/// Парсер Kraken-подобного формата-массива: [channelId, [price, volume, time_sec], "trade", pair, tradeId].
/// Ручной Utf8JsonReader — формат позиционный, на DTO не ложится.
/// </summary>
public sealed class KrakenMessageParser : IMessageParser
{
    public bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick)
    {
        tick = default;
        try
        {
            var reader = new Utf8JsonReader(utf8Frame);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray) return false;

            // [0] channelId — пропускаем
            if (!reader.Read() || reader.TokenType != JsonTokenType.Number) return false;

            // [1] вложенный массив [price, volume, time]
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray) return false;
            if (!ReadDecimalString(ref reader, out var price)) return false;
            if (!ReadDecimalString(ref reader, out var volume)) return false;
            if (!ReadDecimalString(ref reader, out var timeSec)) return false;
            if (!reader.Read() || reader.TokenType != JsonTokenType.EndArray) return false;

            // [2] "trade"
            if (!reader.Read() || reader.TokenType != JsonTokenType.String) return false;

            // [3] pair "BTC/USD"
            if (!reader.Read() || reader.TokenType != JsonTokenType.String) return false;
            var symbol = reader.GetString();
            if (string.IsNullOrEmpty(symbol)) return false;

            // [4] tradeId
            if (!reader.Read() || reader.TokenType != JsonTokenType.Number || !reader.TryGetInt64(out var tradeId))
                return false;

            var ms = (long)(timeSec * 1000m);
            tick = new Tick(Exchange.Kraken, symbol, price, volume,
                DateTimeOffset.FromUnixTimeMilliseconds(ms), tradeId, ingestTimestamp);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool ReadDecimalString(ref Utf8JsonReader reader, out decimal value)
    {
        value = 0m;
        if (!reader.Read() || reader.TokenType != JsonTokenType.String) return false;
        var s = reader.GetString();
        return decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }
}
```

- [ ] **Step 4: Запустить → PASS**, 0 warnings. Commit.

```bash
git add src/SeniorTicker.Infrastructure.WebSockets/Parsers/KrakenMessageParser.cs tests/SeniorTicker.WebSockets.Tests/KrakenMessageParserTests.cs
git commit -m "feat(ws): KrakenMessageParser (positional array via Utf8JsonReader)"
```

---

### Task 4: `CsvMessageParser` (TDD) — текстовый формат

**Files:** Create `src/SeniorTicker.Infrastructure.WebSockets/Parsers/CsvMessageParser.cs`, `tests/SeniorTicker.WebSockets.Tests/CsvMessageParserTests.cs`

- [ ] **Step 1: Падающие тесты `CsvMessageParserTests.cs`**

```csharp
using System.Text;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class CsvMessageParserTests
{
    private static ReadOnlySpan<byte> Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static readonly DateTimeOffset Ingest = DateTimeOffset.UnixEpoch;

    [Fact]
    public void Parses_valid_csv_line()
    {
        // symbol,price,volume,epochMs,id
        var parser = new CsvMessageParser();
        Assert.True(parser.TryParse(Utf8("ETHUSD,3000.25,1.5,1700000000000,777"), Ingest, out var tick));
        Assert.Equal(Exchange.Coinbase, tick.Exchange);
        Assert.Equal("ETHUSD", tick.Symbol);
        Assert.Equal(3000.25m, tick.Price);
        Assert.Equal(1.5m, tick.Volume);
        Assert.Equal(777, tick.SourceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), tick.ExchangeTimestamp);
    }

    [Theory]
    [InlineData("")]
    [InlineData("ETHUSD,3000.25,1.5")]                 // мало полей
    [InlineData("ETHUSD,bad,1.5,1700000000000,1")]     // цена не число
    [InlineData(",3000,1.5,1700000000000,1")]          // пустой символ
    public void Rejects_malformed(string raw)
    {
        var parser = new CsvMessageParser();
        Assert.False(parser.TryParse(Utf8(raw), Ingest, out _));
    }
}
```

- [ ] **Step 2: Запустить → FAIL.**

- [ ] **Step 3: `Parsers/CsvMessageParser.cs`** (разбор по UTF-8 без аллокаций строк где можно)

```csharp
using System.Buffers.Text;
using System.Globalization;
using System.Text;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>Парсер CSV-кадра: symbol,price,volume,epochMs,id. Демонстрирует не-JSON формат.</summary>
public sealed class CsvMessageParser : IMessageParser
{
    public bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick)
    {
        tick = default;
        Span<Range> fields = stackalloc Range[6];
        var text = utf8Frame;
        var count = Split(text, (byte)',', fields);
        if (count != 5) return false;

        var symbolBytes = text[fields[0]];
        if (symbolBytes.IsEmpty) return false;
        var symbol = Encoding.UTF8.GetString(symbolBytes);

        if (!TryParseDecimal(text[fields[1]], out var price)) return false;
        if (!TryParseDecimal(text[fields[2]], out var volume)) return false;
        if (!Utf8Parser.TryParse(text[fields[3]], out long epochMs, out _)) return false;
        if (!Utf8Parser.TryParse(text[fields[4]], out long id, out _)) return false;

        tick = new Tick(Exchange.Coinbase, symbol, price, volume,
            DateTimeOffset.FromUnixTimeMilliseconds(epochMs), id, ingestTimestamp);
        return true;
    }

    private static int Split(ReadOnlySpan<byte> s, byte sep, Span<Range> ranges)
    {
        var n = 0; var start = 0;
        for (var i = 0; i < s.Length && n < ranges.Length; i++)
        {
            if (s[i] == sep) { ranges[n++] = new Range(start, i); start = i + 1; }
        }
        if (n < ranges.Length) ranges[n++] = new Range(start, s.Length);
        return n;
    }

    private static bool TryParseDecimal(ReadOnlySpan<byte> utf8, out decimal value)
        => decimal.TryParse(utf8, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
}
```

- [ ] **Step 4: Запустить → PASS**, 0 warnings. Commit.

```bash
git add src/SeniorTicker.Infrastructure.WebSockets/Parsers/CsvMessageParser.cs tests/SeniorTicker.WebSockets.Tests/CsvMessageParserTests.cs
git commit -m "feat(ws): CsvMessageParser (UTF-8 span split)"
```

---

### Task 5: `TickValidator` (TDD) — гейт значений (безопасность §11)

**Files:** Create `src/SeniorTicker.Infrastructure.WebSockets/TickValidator.cs`, `tests/SeniorTicker.WebSockets.Tests/TickValidatorTests.cs`

- [ ] **Step 1: Падающие тесты `TickValidatorTests.cs`**

```csharp
using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class TickValidatorTests
{
    private static readonly DateTimeOffset Now = new(2026, 5, 29, 12, 0, 0, TimeSpan.Zero);

    private static Tick Make(decimal price = 100m, decimal volume = 1m,
        DateTimeOffset? ts = null, string symbol = "BTCUSDT")
        => new(Exchange.Binance, symbol, price, volume, ts ?? Now, 1, Now);

    private static FakeTimeProvider Time() => new(Now);

    [Fact] public void Accepts_a_sane_tick() => Assert.True(TickValidator.IsValid(Make(), Time()));

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Rejects_non_positive_price(decimal price)
        => Assert.False(TickValidator.IsValid(Make(price: price), Time()));

    [Fact] public void Rejects_negative_volume() => Assert.False(TickValidator.IsValid(Make(volume: -1m), Time()));
    [Fact] public void Rejects_empty_symbol() => Assert.False(TickValidator.IsValid(Make(symbol: ""), Time()));

    [Fact]
    public void Rejects_timestamp_far_in_the_past()
        => Assert.False(TickValidator.IsValid(Make(ts: Now - TimeSpan.FromDays(8)), Time()));

    [Fact]
    public void Rejects_timestamp_far_in_the_future()
        => Assert.False(TickValidator.IsValid(Make(ts: Now + TimeSpan.FromMinutes(2)), Time()));
}
```

- [ ] **Step 2: Запустить → FAIL.**

- [ ] **Step 3: `TickValidator.cs`**

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Гейт значений недоверенного фида (§11). decimal исключает NaN/Infinity по типу; проверяем
/// диапазон цены/объёма, окно времени (защищает окно дедупа и тайм-партиции от абсурдных меток) и символ.
/// </summary>
public static class TickValidator
{
    public static readonly TimeSpan MaxPastSkew = TimeSpan.FromDays(7);
    public static readonly TimeSpan MaxFutureSkew = TimeSpan.FromMinutes(1);

    public static bool IsValid(in Tick tick, TimeProvider time)
    {
        if (tick.Price <= 0m) return false;
        if (tick.Volume < 0m) return false;
        if (string.IsNullOrEmpty(tick.Symbol)) return false;

        var now = time.GetUtcNow();
        if (tick.ExchangeTimestamp < now - MaxPastSkew) return false;
        if (tick.ExchangeTimestamp > now + MaxFutureSkew) return false;
        return true;
    }
}
```

- [ ] **Step 4: Запустить → PASS**, 0 warnings. Commit.

```bash
git add src/SeniorTicker.Infrastructure.WebSockets/TickValidator.cs tests/SeniorTicker.WebSockets.Tests/TickValidatorTests.cs
git commit -m "feat(ws): TickValidator value gate (price/volume/symbol/time-window)"
```

---

### Task 6: `WebSocketMessageReceiver` + `ReceivedMessage` + `FakeWebSocket` (TDD) — фикс #8

**Files:** Create `src/SeniorTicker.Infrastructure.WebSockets/ReceivedMessage.cs`, `WebSocketMessageReceiver.cs`; add `FakeWebSocket` to `tests/SeniorTicker.WebSockets.Tests/TestDoubles.cs`; Create `tests/SeniorTicker.WebSockets.Tests/WebSocketMessageReceiverTests.cs`

- [ ] **Step 1: `ReceivedMessage.cs`** (владение pooled-буфером, без per-message аллокации payload)

```csharp
using System.Buffers;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Полученное WS-сообщение поверх арендованного из ArrayPool буфера. Span не пересекает await
/// (парсинг синхронный после receive). Dispose возвращает буфер в пул — НЕ использовать Span после Dispose.
/// </summary>
public readonly struct ReceivedMessage : IDisposable
{
    private readonly byte[]? _rented;
    private readonly int _length;

    public bool IsClosed { get; }

    private ReceivedMessage(byte[]? rented, int length, bool isClosed)
    {
        _rented = rented;
        _length = length;
        IsClosed = isClosed;
    }

    public static ReceivedMessage Message(byte[] rented, int length) => new(rented, length, false);
    public static readonly ReceivedMessage Closed = new(null, 0, true);

    public ReadOnlySpan<byte> Span => _rented is null ? default : _rented.AsSpan(0, _length);

    public void Dispose()
    {
        if (_rented is not null)
            ArrayPool<byte>.Shared.Return(_rented);
    }
}
```

- [ ] **Step 2: `WebSocketMessageReceiver.cs`** (растущий буфер до `MaxMessageBytes` — фикс #8)

```csharp
using System.Buffers;
using System.Net.WebSockets;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Читает ОДНО полное WS-сообщение в растущий ArrayPool-буфер. Фикс #8: фиксированный буфер
/// молча терял/обрывал сообщения больше его размера; здесь буфер растёт по мере накопления
/// фрагментов до EndOfMessage, а превышение MaxMessageBytes — ЯВНОЕ исключение (а не молчаливое
/// усечение и не безграничный рост → защита от memory-DoS).
/// </summary>
public sealed class WebSocketMessageReceiver(int initialBufferBytes, int maxMessageBytes)
{
    public async ValueTask<ReceivedMessage> ReceiveAsync(WebSocket socket, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(initialBufferBytes);
        var total = 0;
        try
        {
            while (true)
            {
                if (total == buffer.Length)
                {
                    if (buffer.Length >= maxMessageBytes)
                        throw new InvalidOperationException(
                            $"WS message exceeds MaxMessageBytes={maxMessageBytes}");
                    var newSize = Math.Min(buffer.Length * 2, maxMessageBytes);
                    var bigger = ArrayPool<byte>.Shared.Rent(newSize);
                    Array.Copy(buffer, bigger, total);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = bigger;
                }

                var result = await socket.ReceiveAsync(buffer.AsMemory(total), ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    return ReceivedMessage.Closed;
                }

                total += result.Count;
                if (result.EndOfMessage)
                    return ReceivedMessage.Message(buffer, total); // владение буфером уходит в ReceivedMessage
            }
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }
}
```

- [ ] **Step 3: Добавить `FakeWebSocket` в `TestDoubles.cs`** (скриптованные фреймы для юнит-теста receiver)

```csharp
using System.Net.WebSockets;

namespace SeniorTicker.WebSockets.Tests;

/// <summary>WebSocket-дублёр: отдаёт заранее заданные фрагменты (для теста растущего буфера).</summary>
public sealed class FakeWebSocket : WebSocket
{
    private readonly Queue<(byte[] Data, bool EndOfMessage, bool Close)> _frames;
    public FakeWebSocket(IEnumerable<(byte[] Data, bool EndOfMessage, bool Close)> frames) => _frames = new(frames);

    public override ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken ct)
    {
        if (_frames.Count == 0)
            return ValueTask.FromResult(new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true));
        var (data, eom, close) = _frames.Dequeue();
        if (close)
            return ValueTask.FromResult(new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true));
        var n = Math.Min(data.Length, buffer.Length);
        data.AsSpan(0, n).CopyTo(buffer.Span);
        if (n < data.Length) // фрагмент не влез целиком — вернуть остаток следующим фреймом
            _frames.Enqueue((data[n..], eom, false));
        var thisEom = n == data.Length && eom;
        return ValueTask.FromResult(new ValueWebSocketReceiveResult(n, WebSocketMessageType.Binary, thisEom));
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
```

- [ ] **Step 4: Падающие тесты `WebSocketMessageReceiverTests.cs`**

```csharp
using System.Text;
using SeniorTicker.Infrastructure.WebSockets;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class WebSocketMessageReceiverTests
{
    [Fact]
    public async Task Reassembles_a_multi_fragment_message_larger_than_initial_buffer()
    {
        // 20 KB сообщение при начальном буфере 4 KB — буфер должен вырасти (фикс #8)
        var payload = Encoding.UTF8.GetBytes(new string('x', 20_000));
        var socket = new FakeWebSocket(
        [
            (payload[..8000], false, false),
            (payload[8000..16000], false, false),
            (payload[16000..], true, false),
        ]);
        var receiver = new WebSocketMessageReceiver(initialBufferBytes: 4096, maxMessageBytes: 256 * 1024);

        using var msg = await receiver.ReceiveAsync(socket, CancellationToken.None);

        Assert.False(msg.IsClosed);
        Assert.Equal(20_000, msg.Span.Length);
        Assert.True(msg.Span.SequenceEqual(payload));
    }

    [Fact]
    public async Task Throws_when_message_exceeds_max()
    {
        var payload = Encoding.UTF8.GetBytes(new string('y', 5000));
        var socket = new FakeWebSocket([(payload, false, false), (payload, false, false)]);
        var receiver = new WebSocketMessageReceiver(initialBufferBytes: 1024, maxMessageBytes: 2048);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => { using var _ = await receiver.ReceiveAsync(socket, CancellationToken.None); });
    }

    [Fact]
    public async Task Returns_closed_on_close_frame()
    {
        var socket = new FakeWebSocket([(Array.Empty<byte>(), true, true)]);
        var receiver = new WebSocketMessageReceiver(1024, 2048);
        using var msg = await receiver.ReceiveAsync(socket, CancellationToken.None);
        Assert.True(msg.IsClosed);
    }
}
```

- [ ] **Step 5: Запустить → сначала FAIL (типы не определены), затем после Step 1–2 PASS.**
Run: `dotnet test tests/SeniorTicker.WebSockets.Tests/SeniorTicker.WebSockets.Tests.csproj --filter WebSocketMessageReceiverTests`
Expected: PASS (3 теста).

- [ ] **Step 6: Commit**

```bash
git add src/SeniorTicker.Infrastructure.WebSockets/ReceivedMessage.cs src/SeniorTicker.Infrastructure.WebSockets/WebSocketMessageReceiver.cs tests/SeniorTicker.WebSockets.Tests/TestDoubles.cs tests/SeniorTicker.WebSockets.Tests/WebSocketMessageReceiverTests.cs
git commit -m "feat(ws): growable WebSocketMessageReceiver with MaxMessageBytes cap (fixes #8)"
```

---

### Task 7: `WebSocketConnectorOptions` + `ResiliencePipelineFactory` + `WebSocketConnectorBase`

**Files:** Create `src/SeniorTicker.Infrastructure.WebSockets/WebSocketConnectorOptions.cs`, `ResiliencePipelineFactory.cs`, `WebSocketConnectorBase.cs`

> Эта задача — оркестрация I/O; покрывается интеграционными тестами в Task 9 (юнит-тест reconnect-петли против живого сокета хрупок). Здесь — реализация + сборка.

- [ ] **Step 1: `WebSocketConnectorOptions.cs`**

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

public sealed class WebSocketConnectorOptions
{
    public required string Name { get; init; }
    public required Exchange Exchange { get; init; }
    public required Uri Url { get; init; }
    public int InitialReceiveBufferBytes { get; init; } = 4 * 1024;
    public int MaxMessageBytes { get; init; } = 256 * 1024;
    public TimeSpan ReconnectBaseDelay { get; init; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(30);
}
```

- [ ] **Step 2: `ResiliencePipelineFactory.cs`** (Polly v8 — reconnect forever, backoff+jitter; дефолт `ShouldHandle` исключает OCE → фикс #6)

```csharp
using Polly;
using Polly.Retry;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Reconnect-политика: бесконечный retry с экспоненциальным backoff + jitter. Дефолтный
/// ShouldHandle Polly v8 ретраит любое исключение КРОМЕ OperationCanceledException — то есть
/// отмена (graceful shutdown) распространяется немедленно (фикс #6), а любой сетевой сбой/закрытие
/// → переподключение. onRetry — для логирования.
/// </summary>
public static class ResiliencePipelineFactory
{
    public static ResiliencePipeline CreateReconnectPipeline(
        WebSocketConnectorOptions options, Action<Exception, TimeSpan, int> onRetry)
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = int.MaxValue,
                BackoffType = DelayBackoffType.Exponential,
                Delay = options.ReconnectBaseDelay,
                MaxDelay = options.ReconnectMaxDelay,
                UseJitter = true,
                OnRetry = args =>
                {
                    onRetry(args.Outcome.Exception!, args.RetryDelay, args.AttemptNumber);
                    return default;
                },
            })
            .Build();
    }
}
```

- [ ] **Step 3: `WebSocketConnectorBase.cs`**

```csharp
using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using Polly;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Базовый коннектор: connect → receive (растущий буфер) → parse → validate → ingest, с
/// автопереподключением через Polly. Чисто async (ни одного заблокированного потока). Логирует
/// connect/disconnect/error. Конкретный коннектор задаёт лишь IMessageParser (формат биржи).
/// </summary>
public sealed class WebSocketConnectorBase(
    WebSocketConnectorOptions options,
    IMessageParser parser,
    ITickIngestor ingestor,
    IMetricsSink metrics,
    TimeProvider time,
    ILogger logger,
    Func<ClientWebSocket> socketFactory) : IExchangeConnector
{
    public string Name => options.Name;

    public async Task RunAsync(CancellationToken ct)
    {
        var pipeline = ResiliencePipelineFactory.CreateReconnectPipeline(options, (ex, delay, attempt) =>
            logger.LogWarning(ex, "{Name}: reconnect attempt {Attempt} in {Delay}", Name, attempt, delay));
        try
        {
            await pipeline.ExecuteAsync(async token => await ConnectAndConsumeAsync(token).ConfigureAwait(false), ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("{Name}: stopped (shutdown)", Name);
        }
    }

    private async Task ConnectAndConsumeAsync(CancellationToken ct)
    {
        using var socket = socketFactory();
        await socket.ConnectAsync(options.Url, ct).ConfigureAwait(false);
        logger.LogInformation("{Name}: connected to {Url}", Name, options.Url);

        var receiver = new WebSocketMessageReceiver(options.InitialReceiveBufferBytes, options.MaxMessageBytes);
        while (!ct.IsCancellationRequested)
        {
            using var msg = await receiver.ReceiveAsync(socket, ct).ConfigureAwait(false);
            if (msg.IsClosed)
            {
                logger.LogWarning("{Name}: server closed connection — reconnecting", Name);
                throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely); // → Polly reconnect
            }

            if (!parser.TryParse(msg.Span, time.GetUtcNow(), out var tick)) { metrics.OnDropped(); continue; }
            if (!TickValidator.IsValid(tick, time)) { metrics.OnDropped(); continue; }
            await ingestor.IngestAsync(tick, ct).ConfigureAwait(false);
        }
    }
}
```

- [ ] **Step 4: Сборка**

Run: `dotnet build src/SeniorTicker.Infrastructure.WebSockets/SeniorTicker.Infrastructure.WebSockets.csproj`
Expected: 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add src/SeniorTicker.Infrastructure.WebSockets/WebSocketConnectorOptions.cs src/SeniorTicker.Infrastructure.WebSockets/ResiliencePipelineFactory.cs src/SeniorTicker.Infrastructure.WebSockets/WebSocketConnectorBase.cs
git commit -m "feat(ws): WebSocketConnectorBase with Polly v8 auto-reconnect (fixes #6)"
```

---

### Task 8: `SeniorTicker.MockExchange` — WS-сервер-заглушка (3 формата)

**Files:** Create `src/SeniorTicker.MockExchange/SeniorTicker.MockExchange.csproj`, `MockTickGenerator.cs`, `Program.cs`

- [ ] **Step 1: Создать ASP.NET Core проект + в solution**

```bash
dotnet new web -n SeniorTicker.MockExchange -o src/SeniorTicker.MockExchange
dotnet sln add src/SeniorTicker.MockExchange/SeniorTicker.MockExchange.csproj
```

- [ ] **Step 2: `MockTickGenerator.cs`** (детерминированная генерация + сериализатор на формат)

```csharp
using System.Globalization;
using System.Text;

namespace SeniorTicker.MockExchange;

/// <summary>Генерирует тики и сериализует в формат конкретной биржи. tradeId монотонный.</summary>
public static class MockTickGenerator
{
    public static string Binance(string symbol, long tradeId, long nowMs)
    {
        var price = (50000 + tradeId % 100).ToString(CultureInfo.InvariantCulture);
        return $$"""{"s":"{{symbol}}","p":"{{price}}.50","q":"0.25","T":{{nowMs}},"a":{{tradeId}}}""";
    }

    public static string Kraken(string pair, long tradeId, long nowMs)
    {
        var price = (3000 + tradeId % 100).ToString(CultureInfo.InvariantCulture);
        var timeSec = (nowMs / 1000.0).ToString("F3", CultureInfo.InvariantCulture);
        return $$"""[42,["{{price}}.5","0.10","{{timeSec}}"],"trade","{{pair}}",{{tradeId}}]""";
    }

    public static string Csv(string symbol, long tradeId, long nowMs)
    {
        var price = (100 + tradeId % 50).ToString(CultureInfo.InvariantCulture);
        return $"{symbol},{price}.25,1.5,{nowMs},{tradeId}";
    }
}
```

- [ ] **Step 3: `Program.cs`** (3 WS-эндпоинта; `?dropAfter=N` закрывает соединение после N сообщений — для теста reconnect)

```csharp
using System.Net.WebSockets;
using System.Text;
using SeniorTicker.MockExchange;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseWebSockets();

// Каждый эндпоинт стримит свой формат. ?dropAfter=N → закрыть после N сообщений (тест reconnect).
app.Map("/binance", (HttpContext ctx) => Stream(ctx, "binance",
    (sym, id, ms) => MockTickGenerator.Binance(sym, id, ms), "BTCUSDT"));
app.Map("/kraken", (HttpContext ctx) => Stream(ctx, "kraken",
    (sym, id, ms) => MockTickGenerator.Kraken(sym, id, ms), "BTC/USD"));
app.Map("/csv", (HttpContext ctx) => Stream(ctx, "csv",
    (sym, id, ms) => MockTickGenerator.Csv(sym, id, ms), "ETHUSD"));

app.Run();

static async Task Stream(HttpContext ctx, string name, Func<string, long, long, string> fmt, string symbol)
{
    if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
    var dropAfter = int.TryParse(ctx.Request.Query["dropAfter"], out var d) ? d : int.MaxValue;
    using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    var ct = ctx.RequestAborted;
    long id = 0;
    try
    {
        while (socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var payload = Encoding.UTF8.GetBytes(fmt(symbol, ++id, nowMs));
            await socket.SendAsync(payload, WebSocketMessageType.Text, endOfMessage: true, ct);
            if (id >= dropAfter)
            {
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "dropAfter", ct);
                return;
            }
            await Task.Delay(20, ct); // ~50 сообщений/сек
        }
    }
    catch (OperationCanceledException) { /* клиент отключился */ }
    catch (WebSocketException) { /* соединение разорвано */ }
}
```

- [ ] **Step 4: Сборка + ручная проверка**

Run: `dotnet build src/SeniorTicker.MockExchange/SeniorTicker.MockExchange.csproj`
Expected: 0 warnings.
(Опционально вручную: `dotnet run --project src/SeniorTicker.MockExchange &` затем подключиться `websocat ws://localhost:5xxx/binance` — увидеть поток JSON; остановить.)

- [ ] **Step 5: Commit**

```bash
git add src/SeniorTicker.MockExchange
git commit -m "feat(mock): MockExchange WS server with 3 wire formats + dropAfter reconnect hook"
```

---

### Task 9: Интеграционные тесты — коннектор ↔ живой MockExchange

**Files:** Create `tests/SeniorTicker.Integration.Tests/SeniorTicker.Integration.Tests.csproj`, `ConnectorReceivesAndNormalizesTests.cs`, `ConnectorReconnectTests.cs`

- [ ] **Step 1: Создать проект + ссылки + пакеты**

```bash
dotnet new xunit -n SeniorTicker.Integration.Tests -o tests/SeniorTicker.Integration.Tests
rm tests/SeniorTicker.Integration.Tests/UnitTest1.cs
dotnet sln add tests/SeniorTicker.Integration.Tests/SeniorTicker.Integration.Tests.csproj
dotnet add tests/SeniorTicker.Integration.Tests/SeniorTicker.Integration.Tests.csproj reference src/SeniorTicker.Infrastructure.WebSockets/SeniorTicker.Infrastructure.WebSockets.csproj src/SeniorTicker.MockExchange/SeniorTicker.MockExchange.csproj
dotnet add tests/SeniorTicker.Integration.Tests/SeniorTicker.Integration.Tests.csproj package Microsoft.AspNetCore.Mvc.Testing
dotnet add tests/SeniorTicker.Integration.Tests/SeniorTicker.Integration.Tests.csproj package Microsoft.Extensions.Logging.Abstractions
```
Add `<PackageVersion Include="Microsoft.AspNetCore.Mvc.Testing" Version="10.0.0" />` to `Directory.Packages.props` if needed (match installed ASP.NET Core version).

> `WebApplicationFactory<T>` нужен публичный `Program` класс. В `MockExchange/Program.cs` он top-level — добавить в конец файла `public partial class Program;` (минимальная правка), и сослаться на сборку MockExchange. Для WebSocket-клиента к тест-серверу используем `WebApplicationFactory.Server.CreateWebSocketClient()` который даёт `ws://` через TestServer; коннектор принимает `Func<ClientWebSocket>` — но TestServer не использует реальный `ClientWebSocket`. ПОЭТОМУ для интеграционного теста запускаем MockExchange на реальном Kestrel-порту (а не TestServer) и подключаемся настоящим `ClientWebSocket`. См. шаги ниже.

- [ ] **Step 2: Добавить `public partial class Program;` в конец `src/SeniorTicker.MockExchange/Program.cs`** (чтобы запускать программно).

- [ ] **Step 3: Хелпер запуска MockExchange на реальном порту + тест нормализации `ConnectorReceivesAndNormalizesTests.cs`**

```csharp
using System.Collections.Concurrent;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging.Abstractions;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.Integration.Tests;

internal sealed class CollectingIngestor : ITickIngestor
{
    private readonly ConcurrentQueue<Tick> _t = new();
    public ValueTask IngestAsync(Tick tick, CancellationToken ct) { _t.Enqueue(tick); return ValueTask.CompletedTask; }
    public IReadOnlyCollection<Tick> Ticks => _t.ToArray();
    public int Count => _t.Count;
}

internal sealed class NoopMetrics : IMetricsSink
{
    public void OnReceived(long n = 1) { } public void OnDeduplicated(long n = 1) { }
    public void OnWritten(long n) { } public void OnDropped(long n = 1) { }
}

/// <summary>Запускает MockExchange на свободном localhost-порту реальным Kestrel.</summary>
internal sealed class MockExchangeServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    public string BaseWsUrl { get; }

    private MockExchangeServer(WebApplication app, string baseWsUrl) { _app = app; BaseWsUrl = baseWsUrl; }

    public static async Task<MockExchangeServer> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0"); // 0 = свободный порт
        var app = MockExchangeProgram.Build(builder); // см. примечание ниже
        await app.StartAsync();
        var addr = app.Urls.First().Replace("http://", "ws://", StringComparison.Ordinal);
        return new MockExchangeServer(app, addr);
    }

    public async ValueTask DisposeAsync() => await _app.DisposeAsync();
}

public class ConnectorReceivesAndNormalizesTests
{
    [Fact]
    public async Task Binance_connector_receives_and_normalizes_ticks()
    {
        await using var server = await MockExchangeServer.StartAsync();
        var ingestor = new CollectingIngestor();
        var options = new WebSocketConnectorOptions
        {
            Name = "binance", Exchange = Exchange.Binance, Url = new Uri($"{server.BaseWsUrl}/binance"),
        };
        var connector = new WebSocketConnectorBase(options, new BinanceMessageParser(),
            ingestor, new NoopMetrics(), TimeProvider.System, NullLogger.Instance, () => new ClientWebSocket());

        using var cts = new CancellationTokenSource();
        var run = connector.RunAsync(cts.Token);

        // ждём, пока придёт хотя бы несколько тиков (poll, не sleep)
        await WaitUntil(() => ingestor.Count >= 5, TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        await run;

        Assert.True(ingestor.Count >= 5);
        var t = ingestor.Ticks.First();
        Assert.Equal(Exchange.Binance, t.Exchange);
        Assert.Equal("BTCUSDT", t.Symbol);
        Assert.True(t.Price > 0m);
    }

    internal static async Task WaitUntil(Func<bool> cond, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (cond()) return;
            await Task.Delay(50);
        }
        throw new TimeoutException("условие не выполнилось за отведённое время");
    }
}
```

> **Примечание по запуску MockExchange программно:** чтобы переиспользовать конфигурацию эндпоинтов из обоих мест (entrypoint и тесты), вынести построение приложения в статический метод. Реализовать в Task 8 как: `public static partial class MockExchangeProgram { public static WebApplication Build(WebApplicationBuilder builder) { var app = builder.Build(); app.UseWebSockets(); /* map /binance,/kraken,/csv */ return app; } }` и в `Program.cs` вызвать его. ЕСЛИ это усложняет Task 8 — допустимо в тесте собрать минимальный сервер инлайн, маппящий те же 3 эндпоинта (DRY можно нарушить ради изоляции теста; зафиксировать выбор в отчёте). Согласовать реализацию с фактической структурой `Program.cs`.

- [ ] **Step 4: Тест автопереподключения `ConnectorReconnectTests.cs`**

```csharp
using System.Net.WebSockets;
using Microsoft.Extensions.Logging.Abstractions;
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.WebSockets;
using SeniorTicker.Infrastructure.WebSockets.Parsers;
using Xunit;

namespace SeniorTicker.Integration.Tests;

public class ConnectorReconnectTests
{
    [Fact]
    public async Task Connector_auto_reconnects_after_server_drops_connection()
    {
        await using var server = await MockExchangeServer.StartAsync();
        var ingestor = new CollectingIngestor();
        // dropAfter=3 → сервер закроет соединение после 3 сообщений; коннектор должен переподключиться
        var options = new WebSocketConnectorOptions
        {
            Name = "binance", Exchange = Exchange.Binance,
            Url = new Uri($"{server.BaseWsUrl}/binance?dropAfter=3"),
            ReconnectBaseDelay = TimeSpan.FromMilliseconds(50),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(200),
        };
        var connector = new WebSocketConnectorBase(options, new BinanceMessageParser(),
            ingestor, new NoopMetrics(), TimeProvider.System, NullLogger.Instance, () => new ClientWebSocket());

        using var cts = new CancellationTokenSource();
        var run = connector.RunAsync(cts.Token);

        // если бы reconnect не работал, поток встал бы на 3 тиках; ждём существенно больше 3
        await ConnectorReceivesAndNormalizesTests.WaitUntil(() => ingestor.Count >= 9, TimeSpan.FromSeconds(15));
        await cts.CancelAsync();
        await run;

        Assert.True(ingestor.Count >= 9, $"ожидалось >=9 тиков через несколько реконнектов, получено {ingestor.Count}");
    }
}
```

- [ ] **Step 5: Запустить интеграционные тесты**

Run: `dotnet test tests/SeniorTicker.Integration.Tests/SeniorTicker.Integration.Tests.csproj`
Expected: PASS (2 теста). Прогнать 3× для стабильности. Если тест reconnect флаки из-за таймингов — увеличить timeout в `WaitUntil`, НЕ ослаблять проверку счётчика.

- [ ] **Step 6: Commit**

```bash
git add tests/SeniorTicker.Integration.Tests Directory.Packages.props src/SeniorTicker.MockExchange/Program.cs
git commit -m "test(integration): connector receives/normalizes + auto-reconnects against live MockExchange"
```

---

### Task 10: Финальная проверка плана 2

- [ ] **Step 1: Полная сборка** — `dotnet build SeniorTicker.sln` → 0 warnings, 0 errors.
- [ ] **Step 2: Все тесты** — `dotnet test SeniorTicker.sln` → все PASS (24 из Плана 1 + новые юнит + 2 интеграционных).
- [ ] **Step 3:** Финальный commit (метка):
```bash
git commit --allow-empty -m "chore: complete Plan 2 — ingestion (3-format connectors + MockExchange + reconnect)"
```

---

## Self-Review (выполнено при написании)

**1. Spec coverage:** §3 (Infrastructure.WebSockets, MockExchange проекты) → Tasks 2,7,8; §5.1 async-коннекторы → Task 7; §11 валидация (NaN неприменим для decimal, диапазон/время/символ) + MaxMessageBytes → Tasks 5,6; §15 №6 (Polly v8 не ретраит OCE) → Task 7, №8 (растущий буфер) → Task 6, №9 (enabled-коннекторы из конфига) → отложено в План 4 (Host/DI). Расширяемость (новая биржа = парсер+коннектор) → Tasks 2–4,7. Reconnect → Tasks 7,9.

**2. Placeholder scan:** код полный во всех шагах. Две явно помеченные точки согласования с фактической структурой (`MockExchangeProgram.Build` в Task 9; версия Mvc.Testing) — это инструкции по интеграции, не плейсхолдеры; решение фиксируется исполнителем в отчёте.

**3. Type consistency:** `IMessageParser.TryParse(ReadOnlySpan<byte>, DateTimeOffset, out Tick)`, `ITickIngestor.IngestAsync(Tick, ct)`, `IExchangeConnector{ Name; RunAsync(ct) }`, `WebSocketMessageReceiver(int,int).ReceiveAsync→ReceivedMessage`, `WebSocketConnectorBase(options, parser, ingestor, metrics, time, logger, Func<ClientWebSocket>)`, `Exchange.Coinbase` для CSV — согласованы между задачами и тестами.

**4. Ambiguity:** producer-seam — `ITickIngestor` (не прямой `ChannelWriter`), чтобы WebSockets не зависел от Processing. Один тик на кадр (мульти-трейд — документированное расширение).

## Roadmap (следующие планы)
- **План 3** — `Infrastructure.Persistence.Postgres`: `CopyTickSink` (binary COPY, connection-per-writer → #4), миграции на старте (`IDbContextFactory` → #10), схема (time-партиции, BRIN, UNIQUE backstop). Testcontainers.
- **План 4** — `Host`: DI-композиция, enabled-коннекторы из конфига (→ #9), `ITickIngestor`→`TickPipeline.Input`, метрики консистентный снимок (→ #7), Serilog, оркестрация двухфазного shutdown (`HostOptions.ShutdownTimeout` > drain), wss-валидация старта. End-to-end MockExchange→Postgres.
- **План 5** — нагрузка (100/с + burst, gen2/LOH ≈ 0), безопасность (`AllowDuplicateProperties=false`, дубли-смаглинг), регрессии на все 10 проблем, README-защита (No Vibecoding).

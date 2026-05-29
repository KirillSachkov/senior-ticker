# SeniorTicker — План 1: Foundation & Processing Core

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Построить фундамент solution и ядро конвейера обработки в памяти — single-writer дедуп, батчер N-или-T, шард-роутер и оркестрацию конвейера с корректным дренажом — полностью покрытое юнит-тестами, без зависимости от БД/сети.

**Architecture:** Чистая архитектура: `Domain` (ноль пакетов) ← `Application` (порты) ← `Processing` (Channels-движок). Поток: input `Channel<Tick>` → router `stableHash(Symbol)%P` → P шардов (каждый single-writer дедуп + батчер) → shared `Channel<Tick[]>` → K writer-loop → `ITickSink`. Дренаж — через `Complete()` входа и ожидание завершения (двухфазно), отмена = abort.

**Tech Stack:** .NET 10 (C# 14), System.Threading.Channels, `TimeProvider`, xUnit, `Microsoft.Extensions.TimeProvider.Testing` (FakeTimeProvider), Central Package Management.

**Опорная спека:** `docs/superpowers/specs/2026-05-29-senior-ticker-design.md` (§3 декомпозиция, §5 конкурентность/shutdown, §6 модель данных, §8 шардинг, §15 маппинг проблем).

---

## File Structure (создаётся этим планом)

```
senior-ticker/
├─ global.json                              # пин SDK .NET 10
├─ Directory.Build.props                    # общий TFM/nullable/LangVersion/warnings-as-errors
├─ Directory.Packages.props                 # Central Package Management (версии в одном месте)
├─ SeniorTicker.sln
├─ src/
│  ├─ SeniorTicker.Domain/
│  │  ├─ SeniorTicker.Domain.csproj         # НОЛЬ PackageReference
│  │  ├─ Exchange.cs                         # enum источников
│  │  ├─ Tick.cs                             # readonly record struct (value type)
│  │  ├─ TickKey.cs                          # точный составной ключ дедупа
│  │  └─ StableHash.cs                       # детерминированный FNV-1a, символ→шард
│  ├─ SeniorTicker.Application/
│  │  ├─ SeniorTicker.Application.csproj
│  │  ├─ IDeduplicator.cs                    # порт дедупа (single-writer контракт)
│  │  ├─ ITickSink.cs                        # порт записи батчей
│  │  └─ IMetricsSink.cs                     # порт метрик
│  └─ SeniorTicker.Processing/
│     ├─ SeniorTicker.Processing.csproj
│     ├─ PipelineOptions.cs                  # ручки P/K/capacities/N/T/окно
│     ├─ SlidingWindowDeduplicator.cs        # single-writer, точный ключ, эвикция по времени
│     ├─ ShardWorker.cs                       # дедуп-фильтр + батч N-или-T в одном loop
│     └─ TickPipeline.cs                      # router + P шардов + shared batches + K writers
└─ tests/
   └─ SeniorTicker.Processing.Tests/
      ├─ SeniorTicker.Processing.Tests.csproj
      ├─ TestDoubles.cs                       # InMemoryTickSink, CountingMetricsSink
      ├─ StableHashTests.cs
      ├─ SlidingWindowDeduplicatorTests.cs
      ├─ ShardWorkerTests.cs
      └─ TickPipelineTests.cs
```

---

### Task 1: Solution skeleton + управление пакетами

**Files:**
- Create: `global.json`, `Directory.Build.props`, `Directory.Packages.props`, `SeniorTicker.sln`

- [ ] **Step 1: Проверить установленный SDK .NET 10**

Run: `dotnet --list-sdks`
Expected: присутствует строка `10.0.x`. Если нет — остановиться и установить .NET 10 SDK.

- [ ] **Step 2: Создать `global.json` (пин SDK)**

```json
{
  "sdk": {
    "version": "10.0.100",
    "rollForward": "latestFeature"
  }
}
```

- [ ] **Step 3: Создать `Directory.Build.props` (общие настройки всех проектов)**

```xml
<Project>
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>
  </PropertyGroup>
</Project>
```

- [ ] **Step 4: Создать `Directory.Packages.props` (Central Package Management)**

```xml
<Project>
  <PropertyGroup>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <CentralPackageTransitivePinningEnabled>true</CentralPackageTransitivePinningEnabled>
  </PropertyGroup>
  <ItemGroup>
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="17.12.0" />
    <PackageVersion Include="xunit" Version="2.9.2" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="2.8.2" />
    <PackageVersion Include="Microsoft.Extensions.TimeProvider.Testing" Version="9.5.0" />
  </ItemGroup>
</Project>
```

- [ ] **Step 5: Создать solution и каталоги**

```bash
dotnet new sln -n SeniorTicker
mkdir -p src tests
```

- [ ] **Step 6: Commit**

```bash
git add global.json Directory.Build.props Directory.Packages.props SeniorTicker.sln
git commit -m "build: solution skeleton + central package management (.NET 10)"
```

> Примечание по версиям: если `dotnet restore` на следующих задачах сообщит, что версия пакета не найдена, выполнить `dotnet package search <Id> --take 1` (или посмотреть на nuget.org) и обновить `Version` в `Directory.Packages.props` на актуальную. Версии тут — точки старта, пинятся в одном месте.

---

### Task 2: Domain — `Exchange`, `Tick`, `TickKey`

**Files:**
- Create: `src/SeniorTicker.Domain/SeniorTicker.Domain.csproj`, `Exchange.cs`, `Tick.cs`, `TickKey.cs`

- [ ] **Step 1: Создать проект Domain (ноль пакетов) и добавить в solution**

```bash
dotnet new classlib -n SeniorTicker.Domain -o src/SeniorTicker.Domain
rm src/SeniorTicker.Domain/Class1.cs
dotnet sln add src/SeniorTicker.Domain/SeniorTicker.Domain.csproj
```

- [ ] **Step 2: `Exchange.cs`**

```csharp
namespace SeniorTicker.Domain;

/// <summary>Источник котировок. byte — компактно для value-type Tick.</summary>
public enum Exchange : byte
{
    Unknown = 0,
    Binance = 1,
    Kraken = 2,
    Coinbase = 3,
    Mock = 4,
}
```

- [ ] **Step 3: `TickKey.cs`**

```csharp
namespace SeniorTicker.Domain;

/// <summary>
/// Точный составной ключ дедупликации. SourceId = нативный trade_id/update_id биржи,
/// либо ingest-присвоенный монотонный счётчик (тогда идемпотентность только в рамках процесса).
/// НЕ 32-битный хеш: два РАЗНЫХ тика в одну миллисекунду не схлопываются в "дубликат".
/// </summary>
public readonly record struct TickKey(
    Exchange Exchange,
    string Symbol,
    DateTimeOffset ExchangeTimestamp,
    long SourceId);
```

- [ ] **Step 4: `Tick.cs`**

```csharp
namespace SeniorTicker.Domain;

/// <summary>
/// Нормализованный тик. readonly record struct (value type): ноль per-item heap-аллокаций
/// на горячем пути; хранится в Channel и батч-массиве инлайн, без боксинга. decimal для
/// цены/объёма — точные деньги без float-дрейфа.
/// </summary>
public readonly record struct Tick(
    Exchange Exchange,
    string Symbol,
    decimal Price,
    decimal Volume,
    DateTimeOffset ExchangeTimestamp,
    long SourceId,
    DateTimeOffset IngestTimestamp)
{
    public TickKey Key => new(Exchange, Symbol, ExchangeTimestamp, SourceId);
}
```

- [ ] **Step 5: Сборка**

Run: `dotnet build src/SeniorTicker.Domain/SeniorTicker.Domain.csproj`
Expected: Build succeeded, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/SeniorTicker.Domain
git commit -m "feat(domain): Exchange, Tick (readonly record struct), TickKey (exact composite key)"
```

---

### Task 3: Domain — `StableHash` (TDD)

**Files:**
- Create: `src/SeniorTicker.Domain/StableHash.cs`
- Test: `tests/SeniorTicker.Processing.Tests/StableHashTests.cs` (тест-проект создаётся в Task 5; до тех пор тест добавим вместе с ним — здесь пишем реализацию и проверяем сборкой, тест запустим в Task 5)

- [ ] **Step 1: `StableHash.cs`**

```csharp
using System.Text;

namespace SeniorTicker.Domain;

/// <summary>
/// Детерминированный хеш для маршрутизации символ→шард. НЕ String.GetHashCode:
/// он рандомизирован per-process (с .NET Core) → один символ маппится в РАЗНЫЕ шарды
/// на разных инстансах/рестартах, что ломает инвариант шардинга. FNV-1a 64-bit по
/// UTF-8 байтам. Результат — ulong (неотрицательный по построению), поэтому % не даёт
/// отрицательного индекса (профилактика класса проблемы №3 из код-ревью).
/// </summary>
public static class StableHash
{
    public static ulong Fnv1a64(ReadOnlySpan<char> value)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        Span<byte> buffer = stackalloc byte[256];
        var maxBytes = Encoding.UTF8.GetByteCount(value);
        byte[]? rented = null;
        var bytes = maxBytes <= buffer.Length
            ? buffer[..Encoding.UTF8.GetBytes(value, buffer)]
            : RentAndEncode(value, maxBytes, out rented);

        var hash = offset;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }

        if (rented is not null)
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        return hash;

        static ReadOnlySpan<byte> RentAndEncode(ReadOnlySpan<char> v, int max, out byte[] rented)
        {
            rented = System.Buffers.ArrayPool<byte>.Shared.Rent(max);
            var written = Encoding.UTF8.GetBytes(v, rented);
            return rented.AsSpan(0, written);
        }
    }

    /// <summary>Индекс шарда [0, shardCount). Неотрицательный по построению.</summary>
    public static int ShardOf(string symbol, int shardCount)
    {
        ArgumentException.ThrowIfNullOrEmpty(symbol);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        return (int)(Fnv1a64(symbol) % (ulong)shardCount);
    }
}
```

- [ ] **Step 2: Проверить сборку Domain**

Run: `dotnet build src/SeniorTicker.Domain/SeniorTicker.Domain.csproj`
Expected: Build succeeded, 0 warnings. (Тесты `StableHashTests` запустим в Task 5.)

- [ ] **Step 3: Commit**

```bash
git add src/SeniorTicker.Domain/StableHash.cs
git commit -m "feat(domain): StableHash (deterministic FNV-1a) for symbol->shard routing"
```

---

### Task 4: Application — порты

**Files:**
- Create: `src/SeniorTicker.Application/SeniorTicker.Application.csproj`, `IDeduplicator.cs`, `ITickSink.cs`, `IMetricsSink.cs`

- [ ] **Step 1: Создать проект Application + ссылка на Domain**

```bash
dotnet new classlib -n SeniorTicker.Application -o src/SeniorTicker.Application
rm src/SeniorTicker.Application/Class1.cs
dotnet sln add src/SeniorTicker.Application/SeniorTicker.Application.csproj
dotnet add src/SeniorTicker.Application/SeniorTicker.Application.csproj reference src/SeniorTicker.Domain/SeniorTicker.Domain.csproj
```

- [ ] **Step 2: `IDeduplicator.cs`**

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Дедупликатор одного шарда. КОНТРАКТ: вызывается ровно из одного потока-потребителя
/// (single-writer), поэтому реализация НЕ обязана быть потокобезопасной — это сознательный
/// выбор, убирающий гонку №1 из код-ревью по конструкции (нет общего состояния между потоками).
/// </summary>
public interface IDeduplicator
{
    /// <returns>true — тик уже виден в окне (дубликат, отбросить); false — новый.</returns>
    bool IsDuplicate(in Tick tick);
}
```

- [ ] **Step 3: `ITickSink.cs`**

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Приёмник батчей тиков (адаптер хранилища). Выражен только в доменных типах — никакого
/// Npgsql сквозь интерфейс, чтобы "сменить БД" был дроп-ин. Реализация — connection-per-writer.
/// </summary>
public interface ITickSink
{
    Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct);
}
```

- [ ] **Step 4: `IMetricsSink.cs`**

```csharp
namespace SeniorTicker.Application;

/// <summary>Счётчики конвейера. Реализация — потокобезопасная (Interlocked), т.к. дёргается из многих стадий.</summary>
public interface IMetricsSink
{
    void OnReceived(long n = 1);
    void OnDeduplicated(long n = 1);
    void OnWritten(long n);
    void OnDropped(long n = 1);
}
```

- [ ] **Step 5: Сборка + добавить ссылку в solution-граф**

Run: `dotnet build src/SeniorTicker.Application/SeniorTicker.Application.csproj`
Expected: Build succeeded, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/SeniorTicker.Application
git commit -m "feat(application): ports IDeduplicator, ITickSink, IMetricsSink"
```

---

### Task 5: Processing-проект + тест-проект + тесты `StableHash`

**Files:**
- Create: `src/SeniorTicker.Processing/SeniorTicker.Processing.csproj`
- Create: `tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj`, `TestDoubles.cs`, `StableHashTests.cs`

- [ ] **Step 1: Создать Processing + ссылки**

```bash
dotnet new classlib -n SeniorTicker.Processing -o src/SeniorTicker.Processing
rm src/SeniorTicker.Processing/Class1.cs
dotnet sln add src/SeniorTicker.Processing/SeniorTicker.Processing.csproj
dotnet add src/SeniorTicker.Processing/SeniorTicker.Processing.csproj reference src/SeniorTicker.Application/SeniorTicker.Application.csproj src/SeniorTicker.Domain/SeniorTicker.Domain.csproj
```

- [ ] **Step 2: Создать тест-проект + ссылки + пакеты**

```bash
dotnet new xunit -n SeniorTicker.Processing.Tests -o tests/SeniorTicker.Processing.Tests
rm tests/SeniorTicker.Processing.Tests/UnitTest1.cs
dotnet sln add tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj
dotnet add tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj reference src/SeniorTicker.Processing/SeniorTicker.Processing.csproj
dotnet add tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj package Microsoft.Extensions.TimeProvider.Testing
```

- [ ] **Step 3: `TestDoubles.cs` (тестовые дублёры)**

```csharp
using System.Collections.Concurrent;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Tests;

/// <summary>Собирает все записанные тики (потокобезопасно) для проверок.</summary>
public sealed class InMemoryTickSink : ITickSink
{
    private readonly ConcurrentQueue<Tick> _all = new();
    public int BatchCount;

    public Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        Interlocked.Increment(ref BatchCount);
        foreach (var t in batch.Span)
            _all.Enqueue(t);
        return Task.CompletedTask;
    }

    public IReadOnlyCollection<Tick> All => _all.ToArray();
}

public sealed class CountingMetricsSink : IMetricsSink
{
    public long Received, Deduplicated, Written, Dropped;
    public void OnReceived(long n = 1) => Interlocked.Add(ref Received, n);
    public void OnDeduplicated(long n = 1) => Interlocked.Add(ref Deduplicated, n);
    public void OnWritten(long n) => Interlocked.Add(ref Written, n);
    public void OnDropped(long n = 1) => Interlocked.Add(ref Dropped, n);
}

public static class TickFactory
{
    public static Tick New(string symbol, long sourceId, decimal price = 100m,
        DateTimeOffset? exchangeTs = null, Exchange exchange = Exchange.Mock)
        => new(exchange, symbol, price, 1m,
               exchangeTs ?? DateTimeOffset.UnixEpoch, sourceId, DateTimeOffset.UnixEpoch);
}
```

- [ ] **Step 4: `StableHashTests.cs` — детерминизм и диапазон**

```csharp
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class StableHashTests
{
    [Fact]
    public void Fnv1a64_is_deterministic_across_calls()
    {
        Assert.Equal(StableHash.Fnv1a64("BTCUSDT"), StableHash.Fnv1a64("BTCUSDT"));
        Assert.NotEqual(StableHash.Fnv1a64("BTCUSDT"), StableHash.Fnv1a64("ETHUSDT"));
    }

    [Theory]
    [InlineData("BTCUSDT", 4)]
    [InlineData("ETHUSDT", 1)]
    [InlineData("a", 8)]
    public void ShardOf_is_in_range_and_stable(string symbol, int shards)
    {
        var s1 = StableHash.ShardOf(symbol, shards);
        var s2 = StableHash.ShardOf(symbol, shards);
        Assert.InRange(s1, 0, shards - 1);
        Assert.Equal(s1, s2);
    }

    [Fact]
    public void ShardOf_rejects_invalid_input()
    {
        Assert.Throws<ArgumentException>(() => StableHash.ShardOf("", 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => StableHash.ShardOf("BTC", 0));
    }
}
```

- [ ] **Step 5: Запустить тесты**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj`
Expected: PASS (StableHash). Build succeeded, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/SeniorTicker.Processing tests/SeniorTicker.Processing.Tests
git commit -m "test(domain): StableHash determinism + test doubles + Processing scaffold"
```

---

### Task 6: `SlidingWindowDeduplicator` (TDD) — фикс проблем №1, №2

**Files:**
- Create: `src/SeniorTicker.Processing/SlidingWindowDeduplicator.cs`
- Test: `tests/SeniorTicker.Processing.Tests/SlidingWindowDeduplicatorTests.cs`

- [ ] **Step 1: Написать падающие тесты**

```csharp
using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class SlidingWindowDeduplicatorTests
{
    private static SlidingWindowDeduplicator Make(FakeTimeProvider time, TimeSpan? window = null)
        => new(window ?? TimeSpan.FromMinutes(1), time);

    [Fact]
    public void First_occurrence_is_not_duplicate_second_is()
    {
        var time = new FakeTimeProvider();
        var dedup = Make(time);
        var t = TickFactory.New("BTCUSDT", sourceId: 1);

        Assert.False(dedup.IsDuplicate(t));   // первый раз — новый
        Assert.True(dedup.IsDuplicate(t));    // повтор — дубликат
    }

    [Fact]
    public void Distinct_ticks_same_symbol_same_timestamp_are_NOT_collapsed()
    {
        // КЛАСС ПРОБЛЕМЫ №2: точный ключ с тайбрейкером SourceId — два разных тика
        // в одну миллисекунду должны остаться двумя разными, не "дубликатом".
        var time = new FakeTimeProvider();
        var dedup = Make(time);
        var ts = DateTimeOffset.UnixEpoch;

        Assert.False(dedup.IsDuplicate(TickFactory.New("BTCUSDT", sourceId: 1, exchangeTs: ts)));
        Assert.False(dedup.IsDuplicate(TickFactory.New("BTCUSDT", sourceId: 2, exchangeTs: ts)));
    }

    [Fact]
    public void Key_outside_time_window_is_treated_as_new_again()
    {
        var time = new FakeTimeProvider();
        var dedup = Make(time, window: TimeSpan.FromSeconds(10));
        var t = TickFactory.New("BTCUSDT", sourceId: 1);

        Assert.False(dedup.IsDuplicate(t));
        time.Advance(TimeSpan.FromSeconds(11));  // окно истекло → запись вытеснена
        Assert.False(dedup.IsDuplicate(t));      // снова новый
    }

    [Fact]
    public void Duplicate_within_window_stays_duplicate()
    {
        var time = new FakeTimeProvider();
        var dedup = Make(time, window: TimeSpan.FromSeconds(10));
        var t = TickFactory.New("BTCUSDT", sourceId: 1);

        Assert.False(dedup.IsDuplicate(t));
        time.Advance(TimeSpan.FromSeconds(5));   // ещё в окне
        Assert.True(dedup.IsDuplicate(t));
    }
}
```

- [ ] **Step 2: Запустить — убедиться, что не компилируется/падает**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj`
Expected: FAIL — `SlidingWindowDeduplicator` не определён.

- [ ] **Step 3: Реализация `SlidingWindowDeduplicator.cs`**

```csharp
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Дедуп по скользящему окну времени. SINGLE-WRITER: дёргается из одного потребителя →
/// HashSet/Queue без синхронизации (ноль локов, нет CAS). Точный TickKey (не 32-битный хеш).
/// Эвикция — O(1) амортизированно через FIFO-очередь сроков: время приёма монотонно
/// неубывающее, поэтому expiry в очереди строго упорядочены.
/// </summary>
public sealed class SlidingWindowDeduplicator(TimeSpan window, TimeProvider time) : IDeduplicator
{
    private readonly HashSet<TickKey> _seen = [];
    private readonly Queue<(DateTimeOffset Expiry, TickKey Key)> _expiry = new();

    public bool IsDuplicate(in Tick tick)
    {
        var now = time.GetUtcNow();
        Evict(now);

        if (!_seen.Add(tick.Key))
            return true; // уже в окне

        _expiry.Enqueue((now + window, tick.Key));
        return false;
    }

    private void Evict(DateTimeOffset now)
    {
        while (_expiry.TryPeek(out var head) && head.Expiry <= now)
        {
            _expiry.Dequeue();
            _seen.Remove(head.Key);
        }
    }
}
```

- [ ] **Step 4: Запустить тесты**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter SlidingWindowDeduplicatorTests`
Expected: PASS (4 теста).

- [ ] **Step 5: Commit**

```bash
git add src/SeniorTicker.Processing/SlidingWindowDeduplicator.cs tests/SeniorTicker.Processing.Tests/SlidingWindowDeduplicatorTests.cs
git commit -m "feat(processing): single-writer SlidingWindowDeduplicator (exact key, time eviction) — fixes #1,#2"
```

---

### Task 7: `PipelineOptions` + `ShardWorker` (TDD) — дедуп-фильтр + батч N-или-T

**Files:**
- Create: `src/SeniorTicker.Processing/PipelineOptions.cs`, `src/SeniorTicker.Processing/ShardWorker.cs`
- Test: `tests/SeniorTicker.Processing.Tests/ShardWorkerTests.cs`

- [ ] **Step 1: `PipelineOptions.cs`**

```csharp
namespace SeniorTicker.Processing;

/// <summary>Ручки конвейера. Дефолты — под нагрузку теста (100/с). P/K выкручены в минимум.</summary>
public sealed class PipelineOptions
{
    public int ShardCount { get; init; } = 1;            // P: число single-writer дедуп-лейнов
    public int WriterCount { get; init; } = 2;           // K: число writer-воркеров
    public int IngestCapacity { get; init; } = 1000;     // bounded input (backpressure #1)
    public int ShardCapacity { get; init; } = 1000;      // bounded per-shard
    public int BatchChannelCapacity { get; init; } = 8;  // bounded batches (backpressure #2)
    public int BatchMaxSize { get; init; } = 1000;       // N: флаш по размеру
    public TimeSpan BatchMaxDelay { get; init; } = TimeSpan.FromMilliseconds(100); // T: флаш по времени
    public TimeSpan DedupWindow { get; init; } = TimeSpan.FromMinutes(1);
}
```

- [ ] **Step 2: Написать падающие тесты для `ShardWorker`**

```csharp
using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class ShardWorkerTests
{
    private static (Channel<Tick> src, Channel<Tick[]> sink) MakeChannels()
        => (Channel.CreateBounded<Tick>(1000), Channel.CreateBounded<Tick[]>(64));

    [Fact]
    public async Task Flushes_a_full_batch_by_size_N()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var worker = new ShardWorker(dedup, maxSize: 3, maxDelay: TimeSpan.FromMinutes(10), time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        for (var i = 1; i <= 3; i++)
            await src.Writer.WriteAsync(TickFactory.New("BTC", i));

        var batch = await sink.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, batch.Length);

        src.Writer.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Flushes_a_partial_batch_by_time_T()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var maxDelay = TimeSpan.FromMilliseconds(100);
        var worker = new ShardWorker(dedup, maxSize: 1000, maxDelay, time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1)); // 1 тик, N не достигнут

        var batch = await ReadWithTimeAdvanceAsync(sink.Reader, time, maxDelay);
        Assert.Single(batch);

        src.Writer.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Skips_duplicates_and_counts_them()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var worker = new ShardWorker(dedup, maxSize: 2, maxDelay: TimeSpan.FromMinutes(10), time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1));
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1)); // дубликат — пропустить
        await src.Writer.WriteAsync(TickFactory.New("BTC", 2));

        var batch = await sink.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, batch.Length);                 // только уникальные 1 и 2
        Assert.Equal(1, Interlocked.Read(ref metrics.Deduplicated));

        src.Writer.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Flushes_partial_batch_on_source_completion()
    {
        var time = new FakeTimeProvider();
        var metrics = new CountingMetricsSink();
        var dedup = new SlidingWindowDeduplicator(TimeSpan.FromMinutes(1), time);
        var worker = new ShardWorker(dedup, maxSize: 1000, maxDelay: TimeSpan.FromMinutes(10), time, metrics);
        var (src, sink) = MakeChannels();

        var run = worker.RunAsync(src.Reader, sink.Writer, CancellationToken.None);
        await src.Writer.WriteAsync(TickFactory.New("BTC", 1));
        await src.Writer.WriteAsync(TickFactory.New("BTC", 2));
        src.Writer.Complete();                          // источник завершён до заполнения N

        var batch = await sink.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, batch.Length);                  // неполный батч дофлашен
        await run.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Детерминированно дожидается батча, флашируемого по таймеру: продвигает FakeTimeProvider
    /// и уступает поток, пока воркер не дойдёт до await на таймере и не сработает флаш.
    /// </summary>
    private static async Task<Tick[]> ReadWithTimeAdvanceAsync(
        ChannelReader<Tick[]> reader, FakeTimeProvider time, TimeSpan step)
    {
        for (var i = 0; i < 200; i++)
        {
            if (reader.TryRead(out var b)) return b;
            time.Advance(step);
            await Task.Delay(1);
        }
        throw new TimeoutException("батч по таймеру не пришёл");
    }
}
```

- [ ] **Step 3: Запустить — убедиться, что падает**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter ShardWorkerTests`
Expected: FAIL — `ShardWorker` не определён.

- [ ] **Step 4: Реализация `ShardWorker.cs`**

```csharp
using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Потребитель одного шарда: применяет single-writer дедуп и группирует уникальные тики
/// в батчи (флаш по размеру N ИЛИ по времени T — что раньше; неполный батч флашится на
/// завершении источника). Один экземпляр = один поток-потребитель (инвариант дедупа).
/// НЕ завершает выходной канал — он общий для всех шардов (его завершает TickPipeline).
/// </summary>
public sealed class ShardWorker(
    IDeduplicator dedup,
    int maxSize,
    TimeSpan maxDelay,
    TimeProvider time,
    IMetricsSink metrics)
{
    public async Task RunAsync(
        ChannelReader<Tick> source,
        ChannelWriter<Tick[]> sink,
        CancellationToken ct)
    {
        while (await source.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            var batch = new List<Tick>(maxSize);
            using var timerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var timer = Task.Delay(maxDelay, time, timerCts.Token);

            while (batch.Count < maxSize)
            {
                if (source.TryRead(out var tick))
                {
                    if (dedup.IsDuplicate(tick)) { metrics.OnDeduplicated(); continue; }
                    batch.Add(tick);
                    continue;
                }

                var ready = source.WaitToReadAsync(ct).AsTask();
                var winner = await Task.WhenAny(ready, timer).ConfigureAwait(false);
                if (winner == timer) break;             // флаш по времени T
                if (!await ready.ConfigureAwait(false)) break; // источник завершён
            }

            timerCts.Cancel();
            await ObserveAsync(timer).ConfigureAwait(false);

            if (batch.Count > 0)
                await sink.WriteAsync(batch.ToArray(), ct).ConfigureAwait(false);
        }
    }

    private static async Task ObserveAsync(Task task)
    {
        try { await task.ConfigureAwait(false); }
        catch (OperationCanceledException) { /* ожидаемо при отмене таймера */ }
    }
}
```

- [ ] **Step 5: Запустить тесты**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter ShardWorkerTests`
Expected: PASS (4 теста). Если тест таймера флаки — увеличить cap итераций в `ReadWithTimeAdvanceAsync`.

- [ ] **Step 6: Commit**

```bash
git add src/SeniorTicker.Processing/PipelineOptions.cs src/SeniorTicker.Processing/ShardWorker.cs tests/SeniorTicker.Processing.Tests/ShardWorkerTests.cs
git commit -m "feat(processing): ShardWorker (single-writer dedup + N-or-T batching)"
```

---

### Task 8: `TickPipeline` (TDD) — router + P шардов + K writers + дренаж

**Files:**
- Create: `src/SeniorTicker.Processing/TickPipeline.cs`
- Test: `tests/SeniorTicker.Processing.Tests/TickPipelineTests.cs`

- [ ] **Step 1: Написать падающие тесты**

```csharp
using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class TickPipelineTests
{
    private static TickPipeline Make(InMemoryTickSink sink, CountingMetricsSink metrics,
        FakeTimeProvider time, int shards = 1, int writers = 2)
        => new(new PipelineOptions
        {
            ShardCount = shards,
            WriterCount = writers,
            BatchMaxSize = 100,
            BatchMaxDelay = TimeSpan.FromMilliseconds(50),
            DedupWindow = TimeSpan.FromMinutes(1),
        }, sink, metrics, time);

    [Fact]
    public async Task Writes_all_unique_ticks_and_drains_on_completion()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 4);

        var run = pipeline.RunAsync(CancellationToken.None);

        for (var i = 1; i <= 50; i++)
            await pipeline.Input.WriteAsync(TickFactory.New($"SYM{i % 5}", i));

        // двухфазный дренаж: завершаем вход и ждём полного прокачивания
        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(50, sink.All.Count);                 // ничего не потеряно при дренаже
        Assert.Equal(50, Interlocked.Read(ref metrics.Written));
    }

    [Fact]
    public async Task Deduplicates_repeated_ticks()
    {
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 2);

        var run = pipeline.RunAsync(CancellationToken.None);

        // один и тот же тик трижды + один уникальный
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 1));
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 1));
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 1));
        await pipeline.Input.WriteAsync(TickFactory.New("BTC", 2));

        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, sink.All.Count);
        Assert.Equal(2, Interlocked.Read(ref metrics.Deduplicated));
    }

    [Fact]
    public async Task Same_symbol_always_routes_to_one_shard_preserving_dedup()
    {
        // Дубликаты одного символа должны попасть в ОДИН шард → дедуп остаётся локальным.
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 8);

        var run = pipeline.RunAsync(CancellationToken.None);
        for (var i = 0; i < 10; i++)
            await pipeline.Input.WriteAsync(TickFactory.New("BTCUSDT", sourceId: 1)); // все дубликаты

        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(sink.All);                          // 1 уникальный, 9 отсеяно одним шардом
        Assert.Equal(9, Interlocked.Read(ref metrics.Deduplicated));
    }
}
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter TickPipelineTests`
Expected: FAIL — `TickPipeline` не определён.

- [ ] **Step 3: Реализация `TickPipeline.cs`**

```csharp
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
```

- [ ] **Step 4: Запустить тесты**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter TickPipelineTests`
Expected: PASS (3 теста).

- [ ] **Step 5: Запустить ВСЕ тесты Processing**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj`
Expected: PASS (все). Build succeeded, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/SeniorTicker.Processing/TickPipeline.cs tests/SeniorTicker.Processing.Tests/TickPipelineTests.cs
git commit -m "feat(processing): TickPipeline (shard router + K writers + two-phase drain)"
```

---

### Task 9: Стресс-тест дедуп-инварианта (анти-регрессия проблемы №1)

**Files:**
- Test: `tests/SeniorTicker.Processing.Tests/TickPipelineTests.cs` (добавить метод)

- [ ] **Step 1: Добавить стресс-тест (высокая параллельная нагрузка продюсеров)**

```csharp
    [Fact]
    public async Task High_parallel_producer_load_no_lost_or_duplicate_writes()
    {
        // Много продюсеров параллельно шлют пересекающиеся ключи; роутинг по символу
        // гарантирует, что дубликаты сходятся в один шард → дедуп корректен без локов.
        var sink = new InMemoryTickSink();
        var metrics = new CountingMetricsSink();
        var time = new FakeTimeProvider();
        var pipeline = Make(sink, metrics, time, shards: 8, writers: 4);

        var run = pipeline.RunAsync(CancellationToken.None);

        const int producers = 16, perProducer = 500, uniqueKeys = 1000;
        var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
            {
                var id = (p * perProducer + i) % uniqueKeys; // намеренные пересечения ключей
                await pipeline.Input.WriteAsync(TickFactory.New($"S{id % 20}", id));
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        pipeline.Input.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(30));

        // каждый (symbol=S{id%20}, sourceId=id) уникален; всего uniqueKeys уникальных ключей
        Assert.Equal(uniqueKeys, sink.All.Count);
        Assert.Equal(uniqueKeys, sink.All.Select(t => t.Key).Distinct().Count());
    }
```

- [ ] **Step 2: Запустить тесты**

Run: `dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter High_parallel_producer_load_no_lost_or_duplicate_writes`
Expected: PASS. Прогнать 5 раз для уверенности в отсутствии флакости:
`for i in 1 2 3 4 5; do dotnet test tests/SeniorTicker.Processing.Tests/SeniorTicker.Processing.Tests.csproj --filter High_parallel_producer_load_no_lost_or_duplicate_writes || break; done`

- [ ] **Step 3: Commit**

```bash
git add tests/SeniorTicker.Processing.Tests/TickPipelineTests.cs
git commit -m "test(processing): parallel-load dedup invariant (anti-regression for race #1)"
```

---

### Task 10: Финальная проверка плана 1

- [ ] **Step 1: Полная сборка solution**

Run: `dotnet build SeniorTicker.sln`
Expected: Build succeeded, 0 warnings (TreatWarningsAsErrors), 0 errors.

- [ ] **Step 2: Все тесты**

Run: `dotnet test SeniorTicker.sln`
Expected: PASS все.

- [ ] **Step 3: Финальный commit (метка завершения плана 1)**

```bash
git add -A
git commit -m "chore: complete Plan 1 — foundation & processing core (in-memory pipeline, fully tested)" --allow-empty
```

---

## Self-Review (выполнено при написании плана)

**1. Spec coverage:**
- §3 декомпозиция (Domain/Application/Processing, граф зависимостей) → Tasks 1,2,4,5. (Infrastructure/Host — планы 2–4.)
- §5.1 модель исполнения (single-writer, async) → ShardWorker/TickPipeline (Tasks 7,8).
- §5.2 single-writer дедуп без гонки №1 → Task 6 + стресс Task 9.
- §5.3 backpressure (bounded `Wait`) → TickPipeline каналы (Task 8).
- §5.4 двухфазный дренаж → TickPipeline.RunAsync + тест drain (Task 8).
- §6.1 Tick (struct), §6.2 TickKey (точный ключ+тайбрейкер) → Task 2; анти-№2 тест → Task 6.
- §8 шардинг по символу, стабильный хеш → Task 3 (StableHash) + Task 8 (роутинг).
- §15 №1 (Task 6,9), №2 (Task 6), №3 (StableHash неотрицательный modulo, Task 3).
- **Отложено осознанно (не дыры):** №4,5(БД-часть),6,7,8,9,10 и безопасность — планы 2–5 (там появляется I/O/Host/БД).

**2. Placeholder scan:** плейсхолдеров нет; каждый шаг содержит полный код/команду/ожидаемый результат.

**3. Type consistency:** `IDeduplicator.IsDuplicate(in Tick)`, `ITickSink.WriteBatchAsync(ReadOnlyMemory<Tick>, ct)`, `ShardWorker(IDeduplicator,int,TimeSpan,TimeProvider,IMetricsSink)`, `TickPipeline(PipelineOptions,ITickSink,IMetricsSink,TimeProvider)`, `StableHash.ShardOf(string,int)` — согласованы между задачами и тестами.

---

## Roadmap (следующие планы)
- **План 2** — `MockExchange` (Kestrel WS, 3 формата) + `Infrastructure.WebSockets` (растущий `ArrayPool`-буфер + `MaxMessageBytes` → №8; reconnect через Polly v8 → №6; парсеры/нормализация per-exchange → расширяемость; валидационный гейт → безопасность). Интеграционный тест reconnect.
- **План 3** — `Infrastructure.Persistence.Postgres` (`CopyTickSink` binary COPY connection-per-writer → №4; миграции на старте через `IDbContextFactory` → №10; схема: time-партиции, BRIN, UNIQUE B-tree backstop). Testcontainers.
- **План 4** — `Host` (DI-композиция, конфиг enabled-коннекторов → №9; метрики консистентный снимок → №7; Serilog; оркестрация двухфазного shutdown с `HostOptions.ShutdownTimeout` > drain; архитектурный тест Domain/Application без инфра-зависимостей). End-to-end MockExchange→Postgres.
- **План 5** — нагрузка (100/с + burst, gen2/LOH ≈ 0 в `dotnet-counters`), безопасность (wss-валидация, `AllowDuplicateProperties=false`, value-гейт), регрессии на все 10 проблем, README-защита решений (No Vibecoding).

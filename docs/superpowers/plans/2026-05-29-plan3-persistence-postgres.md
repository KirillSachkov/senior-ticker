# SeniorTicker — План 3: Persistence (PostgreSQL binary COPY)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Реализовать слой хранения raw-тиков в PostgreSQL: `ITickSink` через Npgsql **binary COPY** (connection-per-call, никакого общего `DbContext`/соединения — фикс #4), схему через EF Core 10 миграции, применяемые отдельным инициализатором (фикс #10), и интеграционные тесты против реального Postgres в Testcontainers.

**Architecture:** Новый проект `SeniorTicker.Infrastructure.Persistence.Postgres` реализует уже существующий порт `ITickSink` (Application). Горячий путь записи — `CopyTickSink`: на каждый батч берёт соединение из потокобезопасного `NpgsqlDataSource` (singleton), стримит строки через `BeginBinaryImportAsync`, `CompleteAsync`. EF Core 10 — ТОЛЬКО схема/миграции через `IDbContextFactory` (не на горячем пути). Схема: таблица `ticks`, logged, `fillfactor 100`, **BRIN** по времени, **UNIQUE B-tree** по составному ключу как backstop (фикс класса #2 на уровне БД). Партиционирование по времени — документированный scale-up (raw-SQL), не в дефолтном пути теста.

**Tech Stack:** .NET 10, Npgsql (binary COPY + `NpgsqlDataSource`), EF Core 10 + `Npgsql.EntityFrameworkCore.PostgreSQL` (миграции/чтения), Testcontainers.PostgreSql, xUnit.

**Опора:** spec `docs/superpowers/specs/2026-05-29-senior-ticker-design.md` (§2 стек, §6 модель данных/схема, §7 DB-эффективность, §15 #4/#10). Планы 1–2 в `main`: `ITickSink` уже определён (`Task WriteBatchAsync(ReadOnlyMemory<Tick>, CancellationToken)`); `Tick` — `readonly record struct` (Exchange enum:byte, Symbol string, Price/Volume decimal, ExchangeTimestamp/IngestTimestamp DateTimeOffset, SourceId long).

**Producer-seam напоминание:** Pipeline (План 1) уже дренажит `ITickSink` K writer-воркерами. `CopyTickSink` connection-per-call делает его потокобезопасным под K параллельных вызовов (фикс #4). Host (План 4) свяжет DI.

---

## File Structure

```
src/SeniorTicker.Infrastructure.Persistence.Postgres/
├─ SeniorTicker.Infrastructure.Persistence.Postgres.csproj  # ref Application, Domain; pkg Npgsql, EFCore.PG, EFCore.Design
├─ TickEntity.cs                 # EF entity (класс) для схемы/миграций — отдельно от доменного struct Tick
├─ TickDbContext.cs              # DbContext: маппинг ticks (BRIN, unique, fillfactor через HasAnnotation/SQL)
├─ PostgresOptions.cs            # ConnectionString, WriterCount (для пула)
├─ CopyTickSink.cs               # ITickSink через binary COPY, connection из NpgsqlDataSource per call
├─ DatabaseInitializer.cs        # применяет миграции на старте (IDbContextFactory) — фикс #10
├─ ServiceCollectionExtensions.cs # AddPostgresPersistence(services, config): NpgsqlDataSource singleton, ITickSink, DbContextFactory, initializer
└─ Migrations/                   # EF миграции (генерируются dotnet ef)
tests/SeniorTicker.Persistence.Tests/
├─ SeniorTicker.Persistence.Tests.csproj  # ref Persistence.Postgres; pkg Testcontainers.PostgreSql, xunit
├─ PostgresFixture.cs            # IAsyncLifetime: поднимает Postgres-контейнер, применяет миграции, даёт connection string
├─ CopyTickSinkTests.cs          # пишет батч → строки в БД; типы/значения; пустой батч; большой батч
└─ SchemaTests.cs               # миграция создаёт ticks + unique index + BRIN; уникальный ключ ловит дубль
```

**Замечание про доменный `Tick` (struct) vs EF entity:** EF Core маппит классы, не `readonly record struct` с positional-ctor удобно. Поэтому для СХЕМЫ заводим отдельный `TickEntity` (POCO-класс) — он живёт только в Persistence, Domain не меняется. `CopyTickSink` пишет напрямую из доменного `Tick` через Npgsql (не через EF/entity), так что entity нужен лишь для генерации/валидации схемы и редких чтений. Это сознательно: EF для DDL, Npgsql для горячей записи (§2).

---

### Task 1: Проект Persistence + пакеты + `PostgresOptions` + `TickEntity`

**Files:** Create the csproj, `PostgresOptions.cs`, `TickEntity.cs`

- [ ] **Step 1: Создать проект + ссылки + пакеты (Central Package Management)**

```bash
dotnet new classlib -n SeniorTicker.Infrastructure.Persistence.Postgres -o src/SeniorTicker.Infrastructure.Persistence.Postgres
rm src/SeniorTicker.Infrastructure.Persistence.Postgres/Class1.cs
dotnet sln add src/SeniorTicker.Infrastructure.Persistence.Postgres/SeniorTicker.Infrastructure.Persistence.Postgres.csproj
dotnet add src/SeniorTicker.Infrastructure.Persistence.Postgres/SeniorTicker.Infrastructure.Persistence.Postgres.csproj reference src/SeniorTicker.Application/SeniorTicker.Application.csproj src/SeniorTicker.Domain/SeniorTicker.Domain.csproj
```
Add to `Directory.Packages.props` (resolve actual latest with `dotnet package search <Id> --take 5` if a pin doesn't restore — Npgsql/EFCore.PG track .NET 10, so expect 10.x; Testcontainers 4.x):
```xml
<PackageVersion Include="Npgsql" Version="10.0.0" />
<PackageVersion Include="Npgsql.EntityFrameworkCore.PostgreSQL" Version="10.0.0" />
<PackageVersion Include="Microsoft.EntityFrameworkCore.Design" Version="10.0.0" />
<PackageVersion Include="Microsoft.Extensions.Configuration.Abstractions" Version="10.0.0" />
<PackageVersion Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="10.0.0" />
<PackageVersion Include="Testcontainers.PostgreSql" Version="4.0.0" />
```
Reference in the project: `Npgsql`, `Npgsql.EntityFrameworkCore.PostgreSQL`, `Microsoft.EntityFrameworkCore.Design`, `Microsoft.Extensions.Configuration.Abstractions`, `Microsoft.Extensions.DependencyInjection.Abstractions`.

- [ ] **Step 2: `PostgresOptions.cs`**

```csharp
namespace SeniorTicker.Infrastructure.Persistence.Postgres;

public sealed class PostgresOptions
{
    public required string ConnectionString { get; init; }
    /// <summary>Верхняя граница соединений для записи (NpgsqlDataSource MaxPoolSize headroom).</summary>
    public int MaxWriterConnections { get; init; } = 8;
}
```

- [ ] **Step 3: `TickEntity.cs`** (EF entity для схемы — отдельно от доменного struct)

```csharp
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// EF-сущность для генерации схемы ticks и редких чтений. НЕ используется на горячем пути записи
/// (там — binary COPY из доменного Tick). Отдельный класс, т.к. EF не маппит readonly record struct.
/// </summary>
public sealed class TickEntity
{
    public long Id { get; set; }                 // bigint identity (суррогат для удобства, не ключ дедупа)
    public Exchange Exchange { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal Volume { get; set; }
    public DateTimeOffset ExchangeTimestamp { get; set; }
    public long SourceId { get; set; }
    public DateTimeOffset IngestTimestamp { get; set; }
}
```

- [ ] **Step 4: Build + commit**

Run: `dotnet build src/SeniorTicker.Infrastructure.Persistence.Postgres/SeniorTicker.Infrastructure.Persistence.Postgres.csproj`
Expected: 0 warnings.
```bash
git add src/SeniorTicker.Infrastructure.Persistence.Postgres Directory.Packages.props
git commit -m "feat(pg): Persistence.Postgres project scaffold + PostgresOptions + TickEntity"
```

---

### Task 2: `TickDbContext` + схема (BRIN, UNIQUE backstop, fillfactor) + initial migration

**Files:** Create `TickDbContext.cs`; generate `Migrations/`

- [ ] **Step 1: `TickDbContext.cs`**

```csharp
using Microsoft.EntityFrameworkCore;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

public sealed class TickDbContext(DbContextOptions<TickDbContext> options) : DbContext(options)
{
    public DbSet<TickEntity> Ticks => Set<TickEntity>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        var e = b.Entity<TickEntity>();
        e.ToTable("ticks", t => t.HasAnnotation("Npgsql:StorageParameter:fillfactor", "100")); // строки не UPDATE-ятся
        e.HasKey(x => x.Id);
        e.Property(x => x.Id).UseIdentityByDefaultColumn();
        e.Property(x => x.Exchange).HasColumnName("exchange").HasConversion<short>();
        e.Property(x => x.Symbol).HasColumnName("symbol").HasMaxLength(32);
        e.Property(x => x.Price).HasColumnName("price").HasPrecision(38, 18);
        e.Property(x => x.Volume).HasColumnName("volume").HasPrecision(38, 18);
        e.Property(x => x.ExchangeTimestamp).HasColumnName("exchange_ts");
        e.Property(x => x.SourceId).HasColumnName("source_id");
        e.Property(x => x.IngestTimestamp).HasColumnName("ingest_ts");

        // UNIQUE backstop по точному составному ключу дедупа (фикс класса #2 на уровне БД)
        e.HasIndex(x => new { x.Exchange, x.Symbol, x.ExchangeTimestamp, x.SourceId })
            .IsUnique()
            .HasDatabaseName("ux_ticks_dedup_key");

        // BRIN по времени: крошечный, дёшев на вставку, для range-сканов (§7)
        e.HasIndex(x => x.ExchangeTimestamp)
            .HasDatabaseName("ix_ticks_exchange_ts_brin")
            .HasMethod("brin");
    }
}
```

- [ ] **Step 2: Design-time factory для `dotnet ef`** — add `TickDbContextFactory.cs`:

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>Только для `dotnet ef` (design-time). Runtime использует AddPostgresPersistence.</summary>
public sealed class TickDbContextFactory : IDesignTimeDbContextFactory<TickDbContext>
{
    public TickDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<TickDbContext>()
            .UseNpgsql("Host=localhost;Database=seniorticker;Username=postgres;Password=postgres")
            .Options;
        return new TickDbContext(options);
    }
}
```

- [ ] **Step 3: Установить dotnet-ef (если нужно) и сгенерировать миграцию**

```bash
dotnet tool install --global dotnet-ef 2>/dev/null || dotnet tool update --global dotnet-ef
dotnet ef migrations add Initial \
  --project src/SeniorTicker.Infrastructure.Persistence.Postgres \
  --startup-project src/SeniorTicker.Infrastructure.Persistence.Postgres
```
(Если `dotnet ef` требует startup web host — наш classlib подойдёт через `IDesignTimeDbContextFactory`. Если генератор ругается на BRIN/fillfactor аннотации — проверь, что они корректны для версии EFCore.PG; при необходимости перенеси BRIN/fillfactor в `migrationBuilder.Sql(...)` внутри сгенерированной миграции.)

- [ ] **Step 4: Проверить сгенерированную миграцию** — открыть `Migrations/*_Initial.cs`, убедиться, что создаются таблица `ticks`, `ux_ticks_dedup_key` (unique), BRIN-индекс, fillfactor. Если EFCore.PG не эмитит BRIN/fillfactor — дописать вручную в `Up()` через `migrationBuilder.Sql("CREATE INDEX ix_ticks_exchange_ts_brin ON ticks USING brin (exchange_ts); ALTER TABLE ticks SET (fillfactor=100);")` и удалить из модели соответствующий конфликт.

- [ ] **Step 5: Build + commit**

```bash
dotnet build src/SeniorTicker.Infrastructure.Persistence.Postgres/SeniorTicker.Infrastructure.Persistence.Postgres.csproj
git add src/SeniorTicker.Infrastructure.Persistence.Postgres
git commit -m "feat(pg): TickDbContext schema (BRIN ts, unique dedup-key backstop, fillfactor 100) + Initial migration"
```

---

### Task 3: `DatabaseInitializer` (миграции на старте — фикс #10) + `ServiceCollectionExtensions`

**Files:** Create `DatabaseInitializer.cs`, `ServiceCollectionExtensions.cs`

- [ ] **Step 1: `DatabaseInitializer.cs`**

```csharp
using Microsoft.EntityFrameworkCore;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// Применяет миграции на старте через IDbContextFactory (фикс #10: НЕ EnsureCreated() в конструкторе
/// репозитория — это игнорировало миграции и было sync-I/O не на месте). Вызывается Host-ом до старта writers.
/// </summary>
public sealed class DatabaseInitializer(IDbContextFactory<TickDbContext> factory)
{
    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var db = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        await db.Database.MigrateAsync(ct).ConfigureAwait(false);
    }
}
```

- [ ] **Step 2: `ServiceCollectionExtensions.cs`**

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SeniorTicker.Application;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует: NpgsqlDataSource (singleton, потокобезопасный пул) для горячей записи COPY;
    /// IDbContextFactory (схема/чтения); ITickSink → CopyTickSink; DatabaseInitializer.
    /// </summary>
    public static IServiceCollection AddPostgresPersistence(this IServiceCollection services, PostgresOptions options)
    {
        var dataSource = new NpgsqlDataSourceBuilder(options.ConnectionString)
        {
            // headroom: writers + EF; защита max_connections (§11)
        }.Build();
        services.AddSingleton(dataSource);

        services.AddDbContextFactory<TickDbContext>(o => o.UseNpgsql(dataSource));
        services.AddSingleton(options);
        services.AddSingleton<DatabaseInitializer>();
        services.AddSingleton<ITickSink, CopyTickSink>();
        return services;
    }
}
```

- [ ] **Step 3: Build (CopyTickSink ещё не создан — закомментируй регистрацию ITickSink до Task 4, или создай заглушку). Лучше: переставь — сделай Task 4 до Step 2 регистрации.**

> Порядок исполнения: реализуй `CopyTickSink` (Task 4) ПЕРЕД финальной сборкой `ServiceCollectionExtensions`. Если собираешь Task 3 раньше — временно убери строку `services.AddSingleton<ITickSink, CopyTickSink>();`, верни в Task 4. Зафиксируй в отчёте.

- [ ] **Step 4: Commit** (после Task 4 сборки)

```bash
git add src/SeniorTicker.Infrastructure.Persistence.Postgres/DatabaseInitializer.cs src/SeniorTicker.Infrastructure.Persistence.Postgres/ServiceCollectionExtensions.cs
git commit -m "feat(pg): DatabaseInitializer (migrate-on-start, fixes #10) + DI wiring (NpgsqlDataSource singleton)"
```

---

### Task 4: `CopyTickSink` — binary COPY, connection-per-call (фикс #4)

**Files:** Create `CopyTickSink.cs`

- [ ] **Step 1: `CopyTickSink.cs`**

```csharp
using Npgsql;
using NpgsqlTypes;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// Пишет батч тиков через Npgsql BINARY COPY. Connection-per-call из потокобезопасного NpgsqlDataSource —
/// нет общего соединения/DbContext между K writer-воркерами (фикс #4). COPY ~100x EF SaveChanges (§7).
/// Дедуп — выше по конвейеру (single-writer), поэтому в ticks приходят уникальные ключи; UNIQUE-индекс —
/// backstop (дубль, если он всё же дойдёт, провалит батч — это намеренно «громкий» сигнал, не тихая потеря).
/// </summary>
public sealed class CopyTickSink(NpgsqlDataSource dataSource) : ITickSink
{
    private const string CopyCommand =
        "COPY ticks (exchange, symbol, price, volume, exchange_ts, source_id, ingest_ts) FROM STDIN (FORMAT BINARY)";

    public async Task WriteBatchAsync(ReadOnlyMemory<Tick> batch, CancellationToken ct)
    {
        if (batch.IsEmpty) return;

        await using var conn = await dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var writer = await conn.BeginBinaryImportAsync(CopyCommand, ct).ConfigureAwait(false);

        for (var i = 0; i < batch.Length; i++)
        {
            var t = batch.Span[i];
            await writer.StartRowAsync(ct).ConfigureAwait(false);
            await writer.WriteAsync((short)t.Exchange, NpgsqlDbType.Smallint, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.Symbol, NpgsqlDbType.Varchar, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.Price, NpgsqlDbType.Numeric, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.Volume, NpgsqlDbType.Numeric, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.ExchangeTimestamp, NpgsqlDbType.TimestampTz, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.SourceId, NpgsqlDbType.Bigint, ct).ConfigureAwait(false);
            await writer.WriteAsync(t.IngestTimestamp, NpgsqlDbType.TimestampTz, ct).ConfigureAwait(false);
        }

        await writer.CompleteAsync(ct).ConfigureAwait(false); // без Complete COPY откатывается
    }
}
```

> ⚠ **Span across await:** `batch.Span[i]` внутри цикла с `await` — нельзя держать `ref`/`Span` ЧЕРЕЗ await. Здесь `var t = batch.Span[i]` КОПИРУЕТ value-struct в локальную переменную ДО await — это безопасно (Span не сохраняется между итерациями, индексируется заново каждый раз). Но компилятор может ругаться, что `ReadOnlySpan` нельзя использовать в async-методе вообще, если выражение `batch.Span[i]` материализуется в точке, пересекающей await. На практике `batch.Span[i]` вычисляется и копируется в `t` синхронно до первого await в теле итерации — это допустимо. ЕСЛИ компилятор не пропускает (`CS4007`/подобное): материализуй заранее в массив — `var arr = batch.ToArray();` перед циклом и итерируй `arr` (одна аллокация на батч; приемлемо, но менее идеально). Зафиксируй выбор в отчёте.

- [ ] **Step 2: Build + восстановить регистрацию ITickSink в ServiceCollectionExtensions (Task 3 Step 2)**

Run: `dotnet build SeniorTicker.sln`
Expected: 0 warnings.

- [ ] **Step 3: Commit**

```bash
git add src/SeniorTicker.Infrastructure.Persistence.Postgres/CopyTickSink.cs src/SeniorTicker.Infrastructure.Persistence.Postgres/ServiceCollectionExtensions.cs
git commit -m "feat(pg): CopyTickSink via binary COPY, connection-per-call (fixes #4)"
```

---

### Task 5: Тест-проект + `PostgresFixture` (Testcontainers) + схема-тесты

**Files:** Create test csproj, `PostgresFixture.cs`, `SchemaTests.cs`

- [ ] **Step 1: Создать тест-проект + ссылки + пакеты**

```bash
dotnet new xunit -n SeniorTicker.Persistence.Tests -o tests/SeniorTicker.Persistence.Tests
rm tests/SeniorTicker.Persistence.Tests/UnitTest1.cs
dotnet sln add tests/SeniorTicker.Persistence.Tests/SeniorTicker.Persistence.Tests.csproj
dotnet add tests/SeniorTicker.Persistence.Tests/SeniorTicker.Persistence.Tests.csproj reference src/SeniorTicker.Infrastructure.Persistence.Postgres/SeniorTicker.Infrastructure.Persistence.Postgres.csproj
dotnet add tests/SeniorTicker.Persistence.Tests/SeniorTicker.Persistence.Tests.csproj package Testcontainers.PostgreSql
```

- [ ] **Step 2: `PostgresFixture.cs`** (поднимает контейнер, применяет миграции, чистит таблицу между тестами)

```csharp
using Microsoft.EntityFrameworkCore;
using Npgsql;
using SeniorTicker.Infrastructure.Persistence.Postgres;
using Testcontainers.PostgreSql;
using Xunit;

namespace SeniorTicker.Persistence.Tests;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:17-alpine")
        .Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;
    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        // применяем миграции (как сделает DatabaseInitializer в Host'е)
        var options = new DbContextOptionsBuilder<TickDbContext>().UseNpgsql(ConnectionString).Options;
        await using (var db = new TickDbContext(options))
            await db.Database.MigrateAsync();
        DataSource = new NpgsqlDataSourceBuilder(ConnectionString).Build();
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }

    public async Task TruncateAsync()
    {
        await using var conn = await DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("TRUNCATE ticks RESTART IDENTITY", conn);
        await cmd.ExecuteNonQueryAsync();
    }
}

[CollectionDefinition("postgres")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;
```

- [ ] **Step 3: `SchemaTests.cs`**

```csharp
using Npgsql;
using Xunit;

namespace SeniorTicker.Persistence.Tests;

[Collection("postgres")]
public class SchemaTests(PostgresFixture fx)
{
    [Fact]
    public async Task Migration_creates_ticks_table_with_unique_and_brin_indexes()
    {
        await using var conn = await fx.DataSource.OpenConnectionAsync();

        await using (var cmd = new NpgsqlCommand(
            "SELECT to_regclass('public.ticks')", conn))
            Assert.NotNull(await cmd.ExecuteScalarAsync() as string ?? null switch { _ => "ticks" });

        // unique index по ключу дедупа существует
        await using (var cmd = new NpgsqlCommand(
            "SELECT 1 FROM pg_indexes WHERE tablename='ticks' AND indexname='ux_ticks_dedup_key'", conn))
            Assert.Equal(1, await cmd.ExecuteScalarAsync());

        // BRIN-индекс по времени существует и это именно brin
        await using (var cmd = new NpgsqlCommand(
            "SELECT am.amname FROM pg_class i JOIN pg_am am ON i.relam=am.oid WHERE i.relname='ix_ticks_exchange_ts_brin'", conn))
            Assert.Equal("brin", await cmd.ExecuteScalarAsync() as string);
    }

    [Fact]
    public async Task Unique_dedup_key_rejects_a_true_duplicate()
    {
        await fx.TruncateAsync();
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        const string insert =
            "INSERT INTO ticks (exchange,symbol,price,volume,exchange_ts,source_id,ingest_ts) " +
            "VALUES (1,'BTCUSDT',1,1,'2026-05-29T00:00:00Z',42,'2026-05-29T00:00:00Z')";
        await using (var c1 = new NpgsqlCommand(insert, conn)) await c1.ExecuteNonQueryAsync();
        await using var c2 = new NpgsqlCommand(insert, conn);
        await Assert.ThrowsAsync<PostgresException>(() => c2.ExecuteNonQueryAsync()); // backstop ловит дубль
    }
}
```

- [ ] **Step 4: Запустить (Docker должен быть запущен)**

Run: `dotnet test tests/SeniorTicker.Persistence.Tests/SeniorTicker.Persistence.Tests.csproj`
Expected: PASS. (Первый прогон тянет образ postgres — может занять время.)

- [ ] **Step 5: Commit**

```bash
git add tests/SeniorTicker.Persistence.Tests Directory.Packages.props
git commit -m "test(pg): Testcontainers fixture + schema tests (unique backstop, BRIN)"
```

---

### Task 6: `CopyTickSinkTests` — запись батча и чтение обратно

**Files:** Create `tests/SeniorTicker.Persistence.Tests/CopyTickSinkTests.cs`

- [ ] **Step 1: Тесты**

```csharp
using SeniorTicker.Domain;
using SeniorTicker.Infrastructure.Persistence.Postgres;
using Npgsql;
using Xunit;

namespace SeniorTicker.Persistence.Tests;

[Collection("postgres")]
public class CopyTickSinkTests(PostgresFixture fx)
{
    private static Tick Tick(string symbol, long sourceId, decimal price = 50000.25m)
        => new(Exchange.Binance, symbol, price, 0.5m,
               new DateTimeOffset(2026, 5, 29, 12, 0, 0, TimeSpan.Zero), sourceId,
               new DateTimeOffset(2026, 5, 29, 12, 0, 0, TimeSpan.Zero));

    [Fact]
    public async Task Writes_a_batch_and_rows_are_queryable_with_correct_values()
    {
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        Tick[] batch = [Tick("BTCUSDT", 1), Tick("ETHUSDT", 2, 3000.75m), Tick("BTCUSDT", 3)];

        await sink.WriteBatchAsync(batch, CancellationToken.None);

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using (var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn))
            Assert.Equal(3L, (long)(await count.ExecuteScalarAsync())!);

        await using var cmd = new NpgsqlCommand(
            "SELECT symbol, price, volume, source_id FROM ticks WHERE source_id=2", conn);
        await using var r = await cmd.ExecuteReaderAsync();
        Assert.True(await r.ReadAsync());
        Assert.Equal("ETHUSDT", r.GetString(0));
        Assert.Equal(3000.75m, r.GetDecimal(1));
        Assert.Equal(0.5m, r.GetDecimal(2));
        Assert.Equal(2L, r.GetInt64(3));
    }

    [Fact]
    public async Task Empty_batch_is_a_noop()
    {
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        await sink.WriteBatchAsync(ReadOnlyMemory<Tick>.Empty, CancellationToken.None);
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(0L, (long)(await count.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Large_batch_5000_rows_writes_via_single_copy()
    {
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        var batch = new Tick[5000];
        for (var i = 0; i < batch.Length; i++) batch[i] = Tick("BTCUSDT", i);
        await sink.WriteBatchAsync(batch, CancellationToken.None);
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(5000L, (long)(await count.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Concurrent_writers_each_own_connection_no_corruption()
    {
        // фикс #4: K параллельных вызовов, каждый берёт своё соединение из NpgsqlDataSource
        await fx.TruncateAsync();
        var sink = new CopyTickSink(fx.DataSource);
        var tasks = Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            var batch = new Tick[500];
            for (var i = 0; i < batch.Length; i++) batch[i] = Tick($"S{w}", w * 1000 + i);
            await sink.WriteBatchAsync(batch, CancellationToken.None);
        }));
        await Task.WhenAll(tasks);
        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM ticks", conn);
        Assert.Equal(4000L, (long)(await count.ExecuteScalarAsync())!);
    }
}
```

- [ ] **Step 2: Запустить → PASS** (4 теста). 0 warnings.

- [ ] **Step 3: Commit**

```bash
git add tests/SeniorTicker.Persistence.Tests/CopyTickSinkTests.cs
git commit -m "test(pg): CopyTickSink batch write, empty, large, concurrent-writers (fixes #4 verified)"
```

---

### Task 7: Финальная проверка плана 3

- [ ] **Step 1:** `dotnet build SeniorTicker.sln` → 0 warnings, 0 errors.
- [ ] **Step 2:** `dotnet test SeniorTicker.sln` → все PASS (63 из Планов 1–2 + новые persistence-тесты). Postgres-тесты требуют Docker.
- [ ] **Step 3:** Финальный marker commit:
```bash
git commit --allow-empty -m "chore: complete Plan 3 — Postgres persistence (binary COPY sink + migrations + Testcontainers)"
```

---

## Self-Review (выполнено при написании)

**1. Spec coverage:** §2 (Npgsql COPY + EF только схема) → Tasks 2,4; §6.2/§6.3 (TickKey-эквивалент unique index, BRIN, fillfactor, logged) → Task 2; §6.4 (миграции на старте через IDbContextFactory, не EnsureCreated) → Task 3 (фикс #10); §7 (COPY как главный рычаг) → Task 4; §15 #4 (connection-per-call, нет общего DbContext) → Task 4 + concurrent-тест Task 6; #10 → Task 3. Staging+ON CONFLICT и тайм-партиции — **сознательно отложены** как multi-instance/scale-up (§10), документированы в этом плане как non-goal для single-instance теста.

**2. Placeholder scan:** код полный. Помеченные точки согласования (порядок Task 3↔4 для регистрации ITickSink; BRIN/fillfactor в модели vs raw-SQL миграции; Span-across-await в COPY) — это инструкции по интеграции с фактическим поведением компилятора/EFCore.PG, исполнитель фиксирует выбор. Не плейсхолдеры.

**3. Type consistency:** `ITickSink.WriteBatchAsync(ReadOnlyMemory<Tick>, CancellationToken)` (из Плана 1) реализуется `CopyTickSink`; колонки COPY ↔ `TickDbContext` маппинг (exchange/symbol/price/volume/exchange_ts/source_id/ingest_ts) согласованы; `Exchange` пишется как `short`/Smallint в обоих местах.

**4. Ambiguity:** дедуп — первичный в памяти (План 1), unique-индекс — backstop, который ГРОМКО (исключением) ловит дубль, дошедший до COPY; для single-instance дубли не доходят (тест это и проверяет — happy path без дублей + отдельный тест, что индекс существует и ловит ручной дубль). Staging+ON CONFLICT — не нужен для теста.

## Roadmap (следующие планы)
- **План 4** — `SeniorTicker.Host`: Generic Host; `AddPostgresPersistence` + connectors-from-config (enabled-only → #9); `ITickIngestor` → `TickPipeline.Input`; `DatabaseInitializer` как `IHostedService` ДО writers; `MetricsBackgroundService` (консистентный снимок → #7); Serilog; оркестрация двухфазного shutdown (`HostOptions.ShutdownTimeout` > drain); wss-валидация старта. End-to-end MockExchange → pipeline → Postgres.
- **План 5** — нагрузка (100/с + burst, gen2/LOH ≈ 0, dotnet-counters), безопасность (wss/секреты/`AllowDuplicateProperties`), регрессии на все 10 проблем (по одному тесту на проблему), README-защита (No Vibecoding).

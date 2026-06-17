# SeniorTicker — агрегатор биржевых тиков (reference-решение)

Имитация системы реального времени: несколько WebSocket-клиентов подключаются к биржевым фидам
(у каждой биржи **свой формат**), поток нормализуется → дедуплицируется → пишется в PostgreSQL;
ведётся структурное логирование и счётчики. Нагрузка по ТЗ — **50–100 тиков/сек**.

> **Тезис решения.** Нагрузка занижена **намеренно**. Проверяют не «вывезешь ли поток», а **строишь
> ли корректные конкурентные гарантии и можешь ли защитить каждый выбор** (принцип «No Vibecoding»).
> Поэтому здесь важнее не количество инструментов, а правильные гарантии by design. Эталон построен
> вокруг реального код-ревью одного решения (вердикт ревьюера: «senior по инструментам, middle по
> многопоточности»); найденные там **10 проблем** закрыты конструктивно — см. таблицу ниже.

Стек: **.NET 10 (C# 14)**, `System.Threading.Channels`, **PostgreSQL** (Npgsql binary COPY),
**EF Core 10** (только схема/миграции), **Polly v8**, **Serilog**, xUnit + **Testcontainers**.

Полный дизайн-документ: [`docs/superpowers/specs/2026-05-29-senior-ticker-design.md`](docs/superpowers/specs/2026-05-29-senior-ticker-design.md).

---

## Быстрый старт

**Требования:** .NET 10 SDK; Docker (для integration/persistence-тестов и локального Postgres).

```bash
# 1. локальный Postgres (или свой инстанс)
docker run -d --name ticker-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17-alpine

# 2. секрет подключения — НЕ в appsettings (§ Безопасность). env или user-secrets:
export ConnectionStrings__Postgres="Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres"
#   или:  dotnet user-secrets --project src/SeniorTicker.Host set "ConnectionStrings:Postgres" "<строка>"

# 3. MockExchange (3 формата на /binance, /kraken, /csv) на порту 5000
dotnet run --project src/SeniorTicker.MockExchange --urls http://127.0.0.1:5000

# 4. Host в Development-профиле (коннекторы из appsettings.Development.json смотрят на mock по loopback)
DOTNET_ENVIRONMENT=Development dotnet run --project src/SeniorTicker.Host
```
Host применит миграции, поднимет коннекторы, и раз в секунду в лог пойдут метрики
`in / deduped / written / dropped / gap / channel depth`. Завершение — `Ctrl+C` (двухфазный дренаж).

```bash
dotnet test          # все тесты (integration/persistence требуют Docker)
dotnet build         # 0 warnings (TreatWarningsAsErrors=true)
```

### Визуальная демонстрация горячего символа

Демо поднимает реальный PostgreSQL, сервер с тремя пайплайнами и интерфейс на React:

```bash
docker compose up --build
```

- Интерфейс: http://localhost:5173
- Проверка API: http://localhost:5080/api/health
- PostgreSQL: `localhost:5432`, база `senior_ticker`, пользователь/пароль `postgres/postgres`

В интерфейсе сравниваются три режима:

| Режим | Что показывает |
|---|---|
| `Каналы: шардинг по символу` | базовый вариант: все тики одного символа попадают в один шард; корректно, но горячий символ упирается в одного потребителя |
| `Каналы: шардинг по ключу тика` | тот же Channels-пайплайн, но ключ шардинга = полный ключ дедупликации; один горячий символ распределяется по шардам |
| `TPL Dataflow: шардинг по ключу тика` | вариант на TPL Dataflow с ограниченными блоками, single-writer дедупом внутри каждого шарда и параллельными воркерами записи |

Ручки демо: входная нагрузка, доля горячего символа, доля дублей, количество шардов, количество воркеров записи, размер батча и искусственная задержка записи в БД. Повторное нажатие `Применить и перезапустить` пересобирает все три режима с новым конфигом.

---

## Архитектура

Зависимости направлены **внутрь, к ядру**. `Domain`/`Application` физически без инфра-пакетов →
инфра-тип там не скомпилируется (правило проверяется архитектурным тестом, а не соглашением).

```
Domain  ←  Application  ←  Processing
   ↑            ↑       ←  Infrastructure.WebSockets
   └────────────┴───────←  Infrastructure.Persistence.Postgres
                         ←  Host  (composition root: связывает всё)
MockExchange — dev/integration-заглушка (Kestrel, 3 формата)
```

| Проект | Ответственность |
|---|---|
| `Domain` | `Tick` (readonly record struct), точный `TickKey`, `StableHash` (FNV-1a) — **ноль пакетов** |
| `Application` | Только порты: `IExchangeConnector/IMessageParser/IDeduplicator/ITickSink/IMetricsSink/ITickIngestor` |
| `Processing` | Channels-движок: router → шарды (single-writer дедуп + батчер N-или-T) → writers; двухфазный дренаж |
| `Infrastructure.WebSockets` | Коннектор + 3 парсера + растущий receive-буфер + Polly reconnect + `TickValidator` |
| `Infrastructure.Persistence.Postgres` | `CopyTickSink` (binary COPY) + EF-схема/миграции |
| `Host` | Единственный composition root: DI, конфиг, Serilog, метрики, оркестрация старта/останова |
| `MockExchange` | WS-сервер-заглушка, 3 формата (`/binance` JSON, `/kraken` массив, `/csv`) |

**Поток данных (spine):**
```
коннекторы (async receive → parse → validate → Tick)
        │  ITickIngestor → bounded Channel<Tick>     ← BACKPRESSURE #1
        ▼
router: StableHash(Symbol) % P
        ▼
P шардов: bounded Channel<Tick> → single-writer дедуп (точный TickKey, окно по времени) → батчер (N-или-T)
        │  bounded Channel<Tick[]>                    ← BACKPRESSURE #2
        ▼
K writer-воркеров (каждый своё NpgsqlConnection) → binary COPY → PostgreSQL
```
Дефолт под тест: `P=1`, `K=2`. Архитектура partition-ready; «ручки» выкручены в минимум.

---

## 10 проблем код-ревью → как эталон их закрывает

Каждая проблема закрыта **by design** и зафиксирована регрессионным тестом (доказательство, а не
обещание). Консолидированный артефакт — `tests/SeniorTicker.Host.Tests/TenProblemsRegressionTests.cs`.

| № | Проблема ученика | Решение в эталоне | Регрессионный тест |
|---|---|---|---|
| **1** | Гонка `TryAdd`+`Interlocked.Exchange` в дедупе | **single-writer** (`SingleReader=true`): состояние трогает один поток → гонка невозможна по конструкции, ноль локов | `Problem01_concurrent_producers_cannot_race_the_deduplicator`; `High_parallel_producer_load_no_lost_or_duplicate_writes` |
| **2** | 32-битный хеш-ключ → ложные дубли | точный составной `TickKey` (Exchange, Symbol, Timestamp, **SourceId**-тайбрейкер) | `Problem02_distinct_ticks_in_same_millisecond_are_not_collapsed`; `Distinct_ticks_same_symbol_same_timestamp_are_NOT_collapsed` |
| **3** | Переполнение `int` → отрицательный индекс шарда | FNV-1a → `ulong`, маска `% P` неотрицательна по построению | `Problem03_shard_index_is_always_in_range_for_adversarial_symbols`; `Fnv1a64_matches_golden_vectors` |
| **4** | Общий `DbContext` / captive dependency | **connection-per-writer** (`NpgsqlConnection` не thread-safe) из `NpgsqlDataSource`; EF только схема | `Concurrent_writers_each_own_connection_no_corruption` *(Persistence, Testcontainers)* |
| **5** | Shutdown теряет данные | **двухфазный дренаж** через `Input.Complete()` (не отмена) + drain-дедлайн | `Problem05_shutdown_drains_buffer_without_loss`; `StopAsync_drains_all_buffered_ticks_without_loss`; `Drains_all_ticks_without_loss_under_backpressure` |
| **6** | Polly ловит `OperationCanceledException` | Polly v8 дефолтный `ShouldHandle` исключает OCE → отмена распространяется мгновенно | `Problem06_reconnect_policy_does_not_retry_cancellation`; `Connector_auto_reconnects_after_server_drops_connection` |
| **7** | Рассинхрон метрик (частичный reset) | **консистентный монотонный снимок** (`Interlocked`, читается вместе, не сбрасывается), rate по дельтам | `Problem07_metrics_snapshot_is_consistent_and_non_resetting`; `Concurrent_increments_are_not_lost` |
| **8** | Фикс-буфер 16 КБ → потеря крупных кадров | растущий `ArrayPool`-буфер до `MaxMessageBytes`, превышение → явный abort/reconnect | `Problem08_receiver_grows_past_initial_buffer_and_caps_at_max`; `Reassembles_a_multi_fragment_message_larger_than_initial_buffer` |
| **9** | Debug-коннектор в проде | регистрация **только enabled** + `ValidateOnStart` wss-гейт (`ws://` лишь на loopback → fail boot) | `Problem09_public_ws_connector_fails_boot`; `Enabled_public_ws_fails_boot` |
| **10** | sync `EnsureCreated()` в конструкторе | миграции отдельным `IHostedService` **до** writers, через `IDbContextFactory.MigrateAsync` | `Initialize_applies_migrations_via_factory_and_is_idempotent` *(Persistence, Testcontainers)* |

---

## Конкурентность и graceful shutdown

- **Кто на каком потоке:** коннекторы — чистый `async` (ни одного заблокированного потока на ожидании
  I/O); дедуп+батчер — выделенный consumer-loop на шард (single-writer → ноль локов); writers — `K`
  воркеров, у каждого своё `NpgsqlConnection`. Формула: *async — concurrency без потока-на-ожидание
  (I/O); параллелизм (потоки) — для CPU; мешать (`.Result`/`.Wait()`) → thread-pool starvation.*
- **Почему single-writer бьёт `ConcurrentDictionary`:** гонка №1 — это check-then-act окно между двумя
  атомарными операциями; любая конкурентная структура его оставляет. `SingleReader=true` **убирает**
  окно: один потребитель → обычный `HashSet` без синхронизации. Гонку нельзя «починить» — её делают
  невозможной.
- **Backpressure:** оба канала bounded (`FullMode.Wait`). Медленная БД → `Channel<Tick[]>` полон →
  батчер ждёт → `Channel<Tick>` полон → дедуп тормозит → коннектор реже читает сокет → TCP flow
  control. **Память — константа** `cap₁·sizeof(Tick) + cap₂·avgBatch`, независимая от входной скорости.
- **Двухфазный дренаж (фикс №5/№6) выпадает из LIFO-порядка хостед-сервисов.** Регистрируем
  `DbInit → Metrics → Pipeline → Connectors`; Generic Host останавливает в обратном порядке: коннекторы
  гаснут **первыми** (вход перекрыт), затем `PipelineHostedService` делает `Input.Complete()` и ждёт
  естественный дренаж в пределах `Shutdown:DrainTimeoutSeconds`. `HostOptions.ShutdownTimeout` ставится
  строго больше. Отмена = abort (только при превышении drain). Координация = порядок регистрации + один
  `Complete()`, без таймеров/флагов.

---

## Безопасность (недоверенный WS-фид)

Любой входящий кадр — атакерский (compromised endpoint / MITM / buggy exchange). Три кольца:

1. **Транспорт:** `wss://` only — `ValidateOnStart` валит хост при `ws://` non-loopback (заодно убивает
   debug-коннектор №9); строка подключения Postgres — секрет (env/user-secrets, **никогда** в
   appsettings); сертификаты валидируются дефолтом `ClientWebSocket` (никогда `return true`).
2. **Нормализационный гейт (на коннектор):** `AllowDuplicateProperties=false` (новое в .NET 10) против
   parser-differential smuggling; reject zero/negative-price, negative-volume и **over-range
   price/volume** (≥ 10^18 → иначе overflow `numeric(38,18)` при COPY → краш хоста одним кадром);
   **reject (skip+count)** тик с timestamp вне окна `[now-7d, now+1min]` (абсурдный timestamp отравил бы
   окно дедупа); whitelist символа (`[A-Za-z0-9/._-]`, ≤32) — иначе амплификация памяти через ключ
   дедупа; malformed = **skip+count**, никогда throw из receive-loop (иначе тривиальный DoS на
   бесконечном reconnect).
3. **Ресурсные governors:** растущий буфер до `MaxMessageBytes` (#8); receive idle-timeout
   (анти-slowloris); bounded-каналы (флуд = backpressure, не OOM); `MaxPoolSize ≥ K + headroom`
   (анти-connection-exhaustion, гейтится на старте).

**БД:** COPY с типизированными `WriteAsync<T>` (символ — значение колонки, не SQL-текст → инъекция
структурно невозможна). Least-privilege роли — см. `docs/postgres-least-privilege.sql`.

---

## Семантика доставки (говорим честно)

In-process Channels = **at-most-once при `kill -9`**: всё между «принято из сокета» и «COMMIT COPY» —
в RAM. **Граница durability — ровно COMMIT.**

| Гарантия | Статус |
|---|---|
| No-loss при штатном `SIGTERM` | ✅ двухфазный дренаж |
| Exactly-once **as stored** | ✅ идемпотентный составной ключ + UNIQUE-backstop |
| At-least-once через крэш | ❌ требует Tier-1 spool (документированный путь, не код) |
| Exactly-once **end-to-end** | ❌ **невозможно** — WS-фид без ack/replay; тик, потерянный на проводе до приёма, недетектируем |

Честный предел Tier 0: BLOCK связывает liveness приёма с liveness Postgres — долгий простой БД →
сокет встаёт → биржа отключит как «медленного потребителя». Лечится Tier-1 спулом.

---

## Производительность и рост

- **Главный рычаг — путь записи:** binary COPY ~100× EF `SaveChanges` (бинарный wire-формат, 1
  round-trip/батч, ноль per-row parse).
- **Дисциплина аллокаций:** `Tick` — `readonly record struct` (**sizeof = 88 байт**, ноль per-item
  heap, в канале инлайн без боксинга); парсинг по UTF-8 без транскодинга; `ArrayPool` для receive.
  **Батч-массив держим < 85 000 байт (граница LOH):** `BatchMaxSize = 900` (900×88 = 79 200 < 85 000;
  при 1000 было бы 88 000 — за LOH → gen2). Проверяется тестом `Batch_default_keeps_array_below_LOH...`
  и нагрузочным `Sustained_high_throughput_no_loss_and_low_gen2`.
- **Growth ladder:** 100/с — корректный скелет; 1k–10k/с — ↑K, батч; 100k/с — **шардинг по символу**
  (каждый шард по-прежнему single-writer!) + time-партиции; мульти-инстанс — партиционирование по
  символу (дедуп остаётся локальным) + UNIQUE-backstop + migrate-job. Один hot-символ нельзя
  распараллелить — один символ = одно ядро.

Ручной профиль под нагрузкой:
```bash
dotnet-counters monitor -- dotnet run -c Release --project src/SeniorTicker.Host
# смотреть: gen-2-gc-count и loh-size ≈ 0; gap in/written ≈ 0 (writers успевают)
```

---

## Конфигурация

`appsettings.json` (prod-дефолты, **без секретов**) + `appsettings.Development.json` (mock-коннекторы на
loopback) + env/user-secrets (секреты). Ручки:

| Ключ | Дефолт | Смысл |
|---|---|---|
| `Exchanges[]` (`Name`,`Format`,`Url`,`Enabled`) | `[]` | источники; регистрируются только `Enabled` |
| `Pipeline:ShardCount` / `WriterCount` | 1 / 2 | P шардов / K writer-воркеров |
| `Pipeline:IngestCapacity` / `ShardCapacity` | 1000 / 1000 | ёмкости bounded-каналов (backpressure) |
| `Pipeline:BatchChannelCapacity` | 8 | ёмкость батч-канала |
| `Pipeline:BatchMaxSize` / `BatchMaxDelayMs` | 900 / 100 | флаш N-или-T (900 держит массив < LOH) |
| `Pipeline:DedupWindowSeconds` | 60 | окно дедупа |
| `Shutdown:DrainTimeoutSeconds` | 30 | бюджет дренажа (< `HostOptions.ShutdownTimeout`) |
| `Metrics:IntervalSeconds` | 1 | период публикации метрик |
| `Postgres:MaxWriterConnections` | 8 | `MaxPoolSize` (гейт: ≥ `WriterCount` + 1) |
| `ConnectionStrings:Postgres` | — | **секрет**: env `ConnectionStrings__Postgres` / user-secrets |

> `Symbols[]` намеренно не вводится: mock стримит фиксированный символ, per-symbol-подписка =
> мультиплексирование (growth-path), а не dead-config.

---

## Тестирование

| Уровень | Что проверяет | Запуск |
|---|---|---|
| Unit | дедуп-инвариант, точный ключ, батчер N-или-T, дренаж, метрики, валидаторы, парсеры | `dotnet test` (быстрые, без Docker) |
| 10 проблем | явный proof по каждой из §15 | `TenProblemsRegressionTests` + см. таблицу |
| Load | 100k без потери, gen2/LOH ≈ 0, burst через backpressure | `LoadTests` |
| Integration | reconnect, per-format парсинг, **end-to-end** MockExchange→Host→Postgres | требует **Docker** (Testcontainers) |
| Persistence | COPY-путь, rollback, миграции на старте | требует **Docker** |
| Architecture | `Domain`/`Application` без инфра-зависимостей | в `Processing.Tests` (рефлексией) |

---

## Supply chain и least-privilege БД

- **Воспроизводимость:** Central Package Management (`Directory.Packages.props`) +
  `RestorePackagesWithLockFile=true` → `packages.lock.json` коммитятся. CI: `dotnet restore --locked-mode`.
  Уязвимости: `dotnet list package --vulnerable --include-transitive`.
- **Least-privilege Postgres** (`docs/postgres-least-privilege.sql`): роль `ticker_migrator` (деплой,
  DDL — гоняет миграции) и `ticker_runtime` (рантайм: `INSERT,SELECT` на `ticks`, **без**
  `DELETE/UPDATE/TRUNCATE/DDL`; COPY-in требует INSERT).

---

## Non-goals (явно)

Брокер (Kafka/Redpanda)/Redis-дедуп/координатор, Tier-1 spool, ClickHouse-sink, аналитика поверх raw,
глобальный порядок между символами — **не для теста** (over-engineering на 100/с). Это документированные
пути роста, а не код. Строим single-process, но **partition-ready**: точный ключ содержит `Symbol` →
кандидаты-дубли всегда в одном шарде, поэтому дедуп остаётся локальным single-writer при шардинге;
плюс seam `IDeduplicator`.

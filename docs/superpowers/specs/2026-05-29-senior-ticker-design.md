# Design: SeniorTicker — система агрегации биржевых тиков (reference-решение)

- **Дата:** 2026-05-29
- **Статус:** утверждён к реализации (после ревью пользователя)
- **Назначение:** эталонное senior-решение тестового задания «система сбора/обработки/хранения
  биржевых тиков» + материал для YouTube-разбора.
- **Связанные артефакты:** brain `wiki/senior-csharp-test-stream-ticker.md` (конспект видео),
  `sources/2026-05-29-senior-csharp-test-stream-ticker.md` (текст ТЗ + 10 проблем код-ревью ученика).

---

## 1. Контекст и цель

### 1.1. Что просят буквально
Имитация системы реального времени: 2–3 WebSocket-клиента подключаются к биржевым серверным
вебсокетам, получают поток котировок `(тикер, цена, объём, timestamp)` — **формат у каждой биржи
свой**; поток нормализуется к единому виду и дедуплицируется; raw-тики пишутся в БД; ведётся
логирование ключевых событий и счётчик обработанных тиков. Нагрузка: **50–100 тиков/сек суммарно**.

Требования ТЗ: параллельная работа с несколькими WS-подключениями; чистая архитектура + SOLID;
обработка обрывов соединения и автоматическое переподключение.

Оценивается: многопоточность/async, WS-lifecycle, эффективность БД, расширяемость под новые биржи,
отказоустойчивость, покрытие тестами, **No Vibecoding** (защита каждого решения по безопасности и
производительности).

### 1.2. Настоящая цель (тезис решения)
> Нагрузка занижена **намеренно**. Проверяют не «вывезешь ли поток», а **строишь ли корректные
> конкурентные гарантии и можешь ли объяснить, почему именно так**. Senior-ход — **не больше
> инструментов, а правильные гарантии + способность защитить выбор**.

Опора — реальное код-ревью решения ученика (стек: .NET 9, TPL Dataflow, EF Core/SQLite, Polly,
Serilog; вердикт ревьюера: **«senior по инструментам, middle по многопоточности», уровень Middle+**).
10 найденных проблем — наш каркас: эталон обязан закрыть **все** by design.

### 1.3. Teaching frame: 10 проблем ученика → 4 senior-компетенции
- **A. Корректность конкурентности** — №1 гонка в дедупликаторе, №4 общий `DbContext` на потоки.
- **B. Lifecycle / DI** — №4 captive dependency, №10 sync `EnsureCreated()` в конструкторе.
- **C. Завершение / backpressure** — №5 shutdown теряет данные, №6 Polly ловит `OperationCanceledException`.
- **D. Корректность под нагрузкой/во времени** — №2 32-битный хеш-ключ, №3 переполнение `int`,
  №8 фикс-буфер 16 КБ, №7 рассинхрон метрик, №9 debug-коннектор в проде.

---

## 2. Стек (решения приняты)

| Слой | Выбор | Обоснование |
|---|---|---|
| Платформа | **.NET 10 LTS** (C# 14) | LTS до ноября 2028; field-backed properties, extension blocks, `AllowDuplicateProperties` в STJ. |
| Конвейер | **System.Threading.Channels** (bounded) | Явный backpressure; `SingleReader=true` — видимый контракт владения. Сознательно НЕ TPL Dataflow (см. §5.4). |
| БД | **PostgreSQL** | Time-series append-нагрузка; партиционирование, BRIN, COPY. |
| Запись | **Npgsql binary COPY**, connection-per-writer | ~100× быстрее EF SaveChanges (оценка, §7); `NpgsqlConnection` не thread-safe. |
| Схема/чтения | **EF Core 10** + `IDbContextFactory` | Только миграции и редкие чтения, не на горячем пути. |
| Resilience | **Polly v8** `ResiliencePipeline` | Дефолтный `ShouldHandle` уже исключает `OperationCanceledException`. |
| Hosting | Generic Host + `BackgroundService` | Lifecycle, DI, graceful shutdown. |
| Логи | **Serilog** (structured) | Значения как свойства (анти-log-injection). |
| Тесты | **xUnit + Testcontainers(Postgres)** + `Microsoft.Extensions.TimeProvider.Testing` | Быстрые unit + честные интеграционные; детерминированное время. |
| Dep-mgmt | Central Package Management (`Directory.Packages.props`), `packages.lock.json`, `RestoreLockedMode` | Воспроизводимость, защита от dependency-confusion. |

---

## 3. Архитектура: декомпозиция проектов

Сбалансированный сплит **~7 проектов** (+ тесты). Принцип: каждый проект меняется по **своей**
причине и имеет свой тест-таргет. Все стрелки зависимостей — **внутрь, к ядру**; `Domain`/`Application`
физически не имеют инфра-пакетов → инфра-тип там не скомпилируется.

| Проект | Единственная ответственность | Зависит от | Пакеты |
|---|---|---|---|
| `SeniorTicker.Domain` | `Tick` (readonly record struct), `TickKey` (точный составной ключ), value objects, enums | — | **ноль** |
| `SeniorTicker.Application` | Только **порты**: `IExchangeConnector`, `IMessageParser`, `IDeduplicator`, `ITickSink`, `IMetricsSink` | Domain | `Microsoft.Extensions.*.Abstractions` |
| `SeniorTicker.Processing` | Движок в памяти: обвязка Channels, single-writer дедуп, батчер (N-или-T), `PipelineBackgroundService`, shutdown-координатор | Application, Domain | Channels, Hosting.Abstractions |
| `SeniorTicker.Infrastructure.WebSockets` | Коннекторы по бирже + парсеры форматов + растущий receive-буфер + reconnect-loop (Polly внутри как папка `Resilience/`) | Application, Domain | `System.Net.WebSockets.Client`, Polly, `System.IO.Hashing` |
| `SeniorTicker.Infrastructure.Persistence.Postgres` | `ITickSink` через binary COPY; EF Core миграции/схема; `IDbContextFactory` | Application, Domain | Npgsql, EF Core, Npgsql.EFCore |
| `SeniorTicker.Host` | **Единственный composition root**: DI, конфиг, оркестрация shutdown, `Observability/` (метрики+Serilog) как папка | всё | Serilog, Hosting |
| `SeniorTicker.MockExchange` | WS-сервер-заглушка (Kestrel), 3 разных формата сообщений | — | ASP.NET Core |
| `*.Tests` | `Application.Tests` (быстрые, без контейнеров) / `Integration.Tests` (Testcontainers+Postgres+MockExchange) | — | xUnit, Testcontainers, FakeTimeProvider |

**Решение про гранулярность (анти-cargo-cult):** `Resilience` и `Observability` — **папки**, не
отдельные проекты. Критик предупреждал: 9–10 проектов на тестовом читаются как enterprise-cargo-cult,
а это штрафует «No Vibecoding». Если политики Polly реально разойдутся между WS и БД — выделим
`Infrastructure.Resilience` позже (YAGNI).

### 3.1. Граф зависимостей
```
                 ┌─────────┐
                 │ Domain  │  (ноль пакетов)
                 └────▲────┘
                      │
                 ┌────┴──────┐
                 │Application│  (только порты/интерфейсы)
                 └────▲──────┘
        ┌────────────┼───────────────┬──────────────────┐
   ┌────┴─────┐ ┌────┴──────────┐ ┌──┴────────────────┐ │
   │Processing│ │Infra.WebSockets│ │Infra.Persistence  │ │
   │          │ │                │ │.Postgres          │ │
   └────▲─────┘ └────▲───────────┘ └──▲────────────────┘ │
        └────────────┴────────┬───────┘                  │
                          ┌───┴───┐                        │
                          │ Host  │◄───────────────────────┘
                          └───────┘  (composition root)

   MockExchange ── (dev/integration only) ──► Host / Integration.Tests
```
**Инвариант:** ни один `Infrastructure.*` не ссылается на другой `Infrastructure.*`. Кросс-инфра
контракт = сигнал, что он должен жить портом в `Application`.

### 3.2. Расширяемость (критерии ТЗ — буквально)
- **«Добавить биржу» = O(1):** один класс `: IExchangeConnector` в `WebSockets` + строка в конфиге.
  Другие проекты не перекомпилируют логику → **OCP**.
- **«Сменить БД» = O(1):** `Persistence.ClickHouse : ITickSink`, Host меняет одну строку DI → **DIP/LSP**.
- **Архитектурный тест** (NetArchTest или кастомный xUnit): `Domain`/`Application` не зависят ни от
  одного `Infrastructure.*` — правило зависимостей становится CI-гейтом, а не соглашением.

---

## 4. Поток данных (spine)

```
 3 mock-эндпоинта (РАЗНЫЕ форматы)
   │        │         │
 ┌─▼──┐  ┌──▼─┐   ┌───▼┐    Ingestion: 1 коннектор/источник
 │Conn│  │Conn│   │Conn│    • async ReceiveAsync (ни одного заблок. потока)
 │+Par│  │+Par│   │+Par│    • reconnect-loop (Polly v8)
 └─┬──┘  └──┬─┘   └─┬──┘    • parse → normalize → validate → Tick
   └────────┼───────┘
            ▼  router: stableHash(Symbol) % P
   ┌──────────────────────────┐
   │ P шардов: bounded         │  BACKPRESSURE #1 (FullMode.Wait)
   │ Channel<Tick>            │
   │  └─ single-writer dedup  │  состояние трогает 1 поток → ноль локов
   │     (точный TickKey,     │  гонка №1 невозможна по конструкции
   │      окно по времени)    │
   │  └─ batcher (N или T мс)  │
   └────────────┬─────────────┘
                ▼
   shared bounded Channel<Tick[]>  BACKPRESSURE #2
                │
                ▼  K writer-воркеров, у каждого СВОЁ NpgsqlConnection
   ┌──────────────────────────┐
   │ CopyTickSink (binary COPY)│
   └────────────┬─────────────┘
                ▼
           PostgreSQL (ticks, time-partitioned)

 MetricsBackgroundService → раз в секунду: in/parsed/dedup/written/dropped + channel depth
```
**Дефолт для теста:** `P=1`, `K=2`, `Channel<Tick>` cap ~1000, `Channel<Tick[]>` cap ~8.
Архитектура partition-ready; «ручки» выкручены в минимум.

---

## 5. Модель конкурентности и graceful shutdown (ядро)

### 5.1. Кто на каком потоке
| Стадия | Природа | Исполнение | Почему |
|---|---|---|---|
| Коннекторы | I/O-bound | чистый `async` `await ReceiveAsync` | заблок. поток ≈ 1 МБ стека; `await` возвращает поток в пул → сотни коннекций ≈ горстка потоков |
| Дедуп+батчер | CPU-bound | выделенный consumer-loop на шард (`await foreach ReadAllAsync`) | single-writer → ноль локов, нет CAS/cache-line ping-pong |
| Райтеры | I/O+DB | `K` воркеров, у каждого своё `NpgsqlConnection` | `NpgsqlConnection` не thread-safe → connection-per-writer (фикс №4) |

**Формула для видео:** *async — concurrency без потока-на-ожидание (I/O); параллелизм (потоки) — для
CPU. Мешать нельзя:* `.Result`/`.Wait()` на горячем пути → thread-pool starvation.

### 5.2. Почему single-writer бьёт `ConcurrentDictionary`
Гонка №1 = `TryAdd` + `Interlocked.Exchange` — две атомарные операции, **вместе не атомарны**
(check-then-act окно). Любая конкурентная структура оставляет это окно. Мы его **убираем**:
`SingleReader=true` гарантирует ровно одного потребителя → `Dictionary<TickKey, DateTime>` не нуждается
в синхронизации. **Гонку нельзя «починить» — её делают невозможной по конструкции.**

### 5.3. Backpressure
Оба канала bounded, `FullMode.Wait`. Цепочка при медленной БД:
`Postgres ↓ → Channel<Tick[]> полон → батчер ждёт WriteAsync → Channel<Tick> полон → дедуп
тормозит → коннектор реже читает сокет → TCP flow control → биржа видит медленного потребителя`.

**Гарантия памяти:** `max heap = cap₁·sizeof(Tick) + cap₂·avgBatch` — **константа, независимая от
входной скорости**. Контраст с буфером ученика на 50 000 (№5): тот прятал затык и терял всё на стопе.
На durable-лейне **никогда** `DropOldest` — это тихая потеря данных.

### 5.4. Graceful shutdown — двухфазный дренаж (фикс №5, №6)
Ученик гасил отменой (host-token у всех блоков) → блоки умирали мгновенно, буфер терялся, in-flight
COPY рвался посреди транзакции. Мы:
```
SIGTERM → ApplicationStopping
 ① стоп коннекторов (host-stop token — reconnect прекращаем)
 ② Complete() писателя Channel<Tick>                «больше тиков не будет»
 ③ дедуп-loop'ы доедают ReadAllAsync, флашат финальный неполный батч
 ④ Complete() писателя Channel<Tick[]>
 ⑤ райтеры дренажат остаток, CompleteAsync() каждый открытый COPY, dispose
 ⑥ всё ограничено отдельным drain-CTS (напр. 30с)
```
Три обязательных нюанса (иначе молча ломается под нагрузкой):
- **`HostOptions.ShutdownTimeout` > drain-таймаута** (дефолт 5с мал!); в K8s `terminationGracePeriodSeconds` ещё больше.
- **Два токена:** host-stop гасит коннекторы; **drain-token** (со своим жёстким дедлайном) управляет
  retry райтера — иначе транзиентный сбой Postgres во время дренажа отменит запись последних батчей.
- **Polly v8** по умолчанию не ретраит OCE → отмена распространяется чисто.

### 5.5. Врезка «почему не Dataflow» (для видео)
`TransformBlock`/`ActionBlock` с `MaxDegreeOfParallelism>1` гоняют **один делегат с общим состоянием**
на N потоках пула → ровно структура, сделавшая гонку №1 и общий контекст №4 лёгкими и невидимыми.
Completion/cancellation завязаны на один токен → отсюда потеря данных №5. **Channels делают владение
явным** (`SingleReader=true`). Платим тем, что руками пишем fan-out router и хореографию завершения —
но именно эта явность и есть «No Vibecoding»: каждую строку можно защитить.

---

## 6. Модель данных и схема БД

### 6.1. `Tick` (Domain, `readonly record struct`)
Поля: `Exchange` (enum/byte), `Symbol` (interned string или symbol-id), `Price` (`decimal`),
`Volume` (`decimal`), `ExchangeTimestamp` (`DateTime` UTC), `SourceId` (long), `IngestTimestamp`.

### 6.2. `TickKey` — гибридный составной ключ + фолбэк (критическое решение)
`TickKey = (Exchange, Symbol, ExchangeTimestamp, SourceId)`, где `SourceId` = нативный
`trade_id`/`update_id` биржи (Binance `aggTrade.a`, Kraken, Coinbase дают), иначе — **ingest-присвоенный
монотонный счётчик**.
> **Документируем явно:** фолбэк-ключ идемпотентен **только в рамках одного процесса** (рестарт
> обнуляет счётчик → кросс-рестарт/кросс-инстанс дедуп по нему невалиден). UNIQUE-констрейнт в БД —
> только для бирж с нативным `SourceId`.

Это закрывает класс проблемы №2: точный ключ вместо 32-битного хеша; два **разных** тика в одну
миллисекунду **не** схлопываются (есть тайбрейкер).

### 6.3. Схема `ticks` (разрешение противоречия от критика)
- **Time-range партиционирование** (день/час) → маленькие индексы/вакуум; ретеншн = `DETACH/DROP PARTITION`.
- **BRIN по `ExchangeTimestamp`** (не B-tree) → килобайты на млн строк, ~ноль стоимости вставки.
- **`fillfactor 100`** → строки не `UPDATE`.
- **Logged** (не UNLOGGED) → мандат «не терять тики».
- **Дешёвый UNIQUE B-tree по `TickKey`** — только для бирж с нативным `SourceId`, как backstop
  (на 100/с стоимость незаметна; без него `ON CONFLICT` невозможен).
> **Нельзя одновременно** «плоский COPY + BRIN-only» и «`ON CONFLICT`»: последнее требует UNIQUE
> B-tree. Решение: для теста — плоский COPY + UNIQUE B-tree (backstop) + BRIN (range-сканы).
> Путь COPY-в-staging + `INSERT…ON CONFLICT DO NOTHING` — мульти-инстанс-апгрейд (§9), т.к. COPY
> сам по себе `ON CONFLICT` не умеет.

### 6.4. Схема/миграции (фикс №10)
EF Core миграции применяются на старте отдельным `IHostedService`-инициализатором **до** старта
райтеров (через `IDbContextFactory`), а не `EnsureCreated()` в конструкторе репозитория. В мульти-инстансе
— вынести в отдельный migrate-job (advisory-lock против гонки).

---

## 7. Пропускная способность и память

**База:** на 100/с система простаивает; боттлнека на пути данных нет. Интересна лестница (§8).

### 7.1. Главный рычаг — путь записи (оценки, проверим бенчем)
| Способ | Относит. скорость | Почему |
|---|---|---|
| EF `SaveChanges` | 1× | change tracking, материализация, аллокации |
| multi-row INSERT (1000 VALUES) | ~10–50× | один statement, но parse/plan/execute |
| **binary COPY** | **~100×+** | бинарный wire-формат, 1 round-trip/батч, ноль per-row parse |

### 7.2. Батч и граница LOH
Батч **~1000–4000 тиков**, массив **< 85 000 байт** (граница LOH; больше → gen2/LOH-сборки). Флаш
**N-или-T** (напр. 1000 **или** 100 мс) — низкая скорость всё равно флашится вовремя. `N`,`T` — конфиг.

### 7.3. Дисциплина аллокаций
`Tick` — `readonly record struct` (ноль per-item heap; канал хранит инлайн, без боксинга);
парсинг — `Utf8JsonReader`/source-gen STJ по UTF-8 без транскодинга; `ArrayPool<byte>` для receive
(rent ≤ 81920, `try/finally`, копируем в struct **до** возврата); интернирование символов.
**Критерий приёмки:** под нагрузкой `dotnet-counters` показывает gen2/LOH ≈ 0 — проверяемо.

### 7.4. Реальный потолок
(1) один потребитель дедупа (~200k–1M lookups/с на ядро — оценка); (2) Postgres WAL/fsync/checkpoint.
Не .NET-рантайм.

---

## 8. Точки роста (growth ladder)

| Масштаб | Что ломается | Рычаг |
|---|---|---|
| **100/с** (спека) | ничего; риск — корректность (10 проблем) | корректный скелет, `P=1,K=2` |
| **1k–10k/с** | аллокации (если class), мелкие батчи | struct Tick, Utf8JsonReader, ↑K, батч N-или-T |
| **100k/с** | один потребитель дедупа упирается в ядро; PG WAL/индексы | **шардинг по символу** `hash(Symbol)%P` (каждый шард по-прежнему single-writer!); time-партиции |
| **1M/с** | один процесс/узел не тянут | горизонталь: символ→инстанс; durable-лог (Kafka/Redpanda); Timescale/Citus |
| **Мульти-инстанс** | in-memory дедуп не видит соседа → дубли; гонка миграций | партиционирование по символу (дедуп локален); UNIQUE-backstop; migrate-job |

**Инсайт:** дедуп масштабируем **не** делая его конкурентным (так ученик поймал №1), а **шардируя
keyspace**: точный ключ всегда содержит `Symbol` → кандидаты-дубли всегда в одном шарде → дедуп
остаётся локальным single-writer. **Партиционирование удаляет проблему распределённого дедупа.**
Жёсткий предел: один hot-символ (BTCUSDT) **нельзя** распараллелить — один символ = одно ядро.
**Хеш — стабильный** (xxHash3/FNV по UTF-8, маскирован в неотрицательное), **НЕ `String.GetHashCode`**
(рандомизирован per-process → ломает шардинг между инстансами/рестартами; маскирование заодно
профилактирует №3).

---

## 9. Семантика доставки и непотеря тиков

### 9.1. Граница durability
In-process Channels = **at-most-once при kill -9**. Всё между «принято из сокета» и «COMMIT COPY» —
в RAM (оба канала + окно дедупа + in-flight батч). **Граница durability — ровно COMMIT.**

### 9.2. Тиры
| Tier | Добавляем | Гарантия |
|---|---|---|
| **Tier 0** (deliverable) | двухфазный дренаж + идемпотентный ключ | at-most-once при крэше; **no-loss при штатном SIGTERM**; exactly-once **as stored** |
| **Tier 1** | локальный append-only spool, fsync до канала | at-least-once через крэш (single-node) |
| **Tier 2** | брокер (Kafka/Redpanda/JetStream), ack после persistence | at-least-once через потерю узла + горизонталь |

**Exactly-once end-to-end невозможно** — WS-фид без ack/replay; тик, потерянный на проводе до приёма,
недетектируем. Говорим прямо.

### 9.3. Backpressure vs drop — явная наблюдаемая политика
Дефолт — **BLOCK** (`Wait`), считаем (`Reader.Count`, время в блокировке). Drop — только явный opt-in
для лоссового лейна, всегда со счётчиком. Тихий drop на durable-лейне — кардинальный грех.

### 9.4. Честный предел Tier 0
BLOCK связывает liveness приёма с liveness Postgres: долгий простой БД → сокет встаёт → биржа
отключает как «медленного потребителя» → «no-loss» деградирует в **невидимый пропуск покрытия**
(тики не приняты, счётчик не ловит). Лечится Tier-1 спулом. Озвучиваем как честную границу.

---

## 10. Мульти-инстанс (growth story, НЕ код для теста)

- **Партиционирование по символу** — первичная ось: каждый символ у ровно одного инстанса → нет
  двойной подписки, in-memory дедуп остаётся корректным, распределённый дедуп **удалён**, а не решён.
- **Назначение WS:** Tier 1 — статическая карта `instance→symbols` в конфиге (диффится, ревьюится,
  ноль runtime-зависимостей); координатор (etcd/Consul/Redis-lease, fencing) — только когда карта
  меняется быстрее, чем человек редактирует.
- **Рост числа коннекций:** мультиплексирование многих символов на один `ClientWebSocket` (combined
  streams), cap streams-per-socket, пул сокетов. ⚠ это рождает крупные агрегированные фреймы →
  обязателен растущий `ArrayPool`-буфер с `MaxMessageBytes` (фикс №8 + энейблер мультиплексирования).
- **DB-дедуп когда нужен:** COPY в UNLOGGED staging → `INSERT…SELECT…ON CONFLICT DO NOTHING`
  (COPY не умеет ON CONFLICT). Применяем только где single-ownership не гарантирован.
- **Split-brain:** при rebalance/network-partition два инстанса пишут один символ → UNIQUE-констрейнт
  обязателен как backstop + fencing-lease (TTL/epoch).
- **Чёткий non-goal для теста:** брокер/Redis-дедуп/координатор — over-engineering на 100/с и SPOF,
  который ревьюер отметит. Строим только single-process, но **partition-ready** (точный ключ,
  `IDeduplicator`-seam, символы из конфига). Мульти-инстанс — документированный путь.

---

## 11. Безопасность (недоверенный WS-фид)

Любой входящий фрейм — атакерский (compromised endpoint / buggy exchange / MITM). Три кольца защиты:

**Кольцо 1 — транспорт:**
- `wss://` only (валидация на старте: ws:// для non-loopback → fail boot, заодно убивает №9
  debug-коннектор); Postgres `SslMode=VerifyFull`; WS `RemoteCertificateValidationCallback` валидирует
  цепочку+SAN, **никогда** `return true`.
- Секреты — вне кода/appsettings: env (prod) / user-secrets (dev); appsettings хранит имена, не значения;
  `IOptions.ValidateOnStart()` → отсутствие секрета = краш на старте, не безмолвная неавторизованная коннекция.

**Кольцо 2 — нормализационный гейт (на коннектор):**
- STJ: `AllowDuplicateProperties=false` (**новое в .NET 10, дефолт `true` — выставить явно!**) против
  parser-differential smuggling (`{"price":1,…,"price":99999}`); `MaxDepth=32`; case-sensitive.
- Валидация значений до канала: reject NaN/Infinity/negative/zero-price/negative-volume; bound numeric
  ranges (конфиг); clamp timestamp в окно `[now-7d, now+1min]` (абсурдный timestamp **отравляет окно
  дедупа** и партиции); `decimal` для цены.
- Malformed-фрейм = **skip + count + sampled-log**, никогда throw из receive-loop (один битый фрейм
  не должен ронять коннектор — тривиальный DoS). Порог → Warning + circuit (Polly).

**Кольцо 3 — ресурсные governors:**
- `MaxMessageBytes` (растущий `ArrayPool`-буфер до cap, дальше abort+reconnect) — фикс №8; контроль —
  именно cap, а не размер буфера (защита от бесконечного фрейма без `EndOfMessage`).
- Receive idle-timeout (linked CTS) + `KeepAliveInterval` — анти-slowloris.
- Bounded-каналы `Wait` → флуд = backpressure, не OOM.

**БД-безопасность:** COPY с типизированными `WriteAsync<T>` (символ — значение в колонке, не SQL-текст
→ инъекция структурно невозможна); least-privilege runtime-роль (INSERT+COPY на `ticks`, без DDL/DELETE/
cross-schema SELECT); отдельная migrator-роль только на деплое; `MaxPoolSize` = K + headroom (анти-
connection-exhaustion).

**Supply chain:** Central Package Management, pin-версии, `packages.lock.json` + `RestoreLockedMode`,
NuGet source mapping, `dotnet list package --vulnerable`/Dependabot в CI.

---

## 12. Мониторинг (фикс №7)

`IMetricsSink` (Interlocked-счётчики) + `MetricsBackgroundService` (раз/сек, Serilog structured).
Метрики: `in / parsed / deduped / written / dropped` + **channel depth** (`Reader.Count`) каждого лейна.
**Консистентный снимок:** все счётчики снимаются/сбрасываются **вместе** (не часть reset, часть нет, как
у ученика). Ключевая величина под нагрузкой — **gap `in` vs `written`** (успевают ли райтеры). Полный
ингест-канал → лимитер дедуп/батч; полный `Tick[]`-канал → лимитер БД. Предпочесть монотонные счётчики
+ дельты (пропущенный scrape не корраптит rate). Логируем connect/disconnect/error источника.

---

## 13. Стратегия тестирования

| Уровень | Что проверяем | Инструменты |
|---|---|---|
| **Unit (быстрые)** | дедуп-инвариант (прогнать 10000× → нет гонки №1); точность ключа (два разных тика в 1 мс **не** схлопнулись — анти-№2); батчер N-или-T; backpressure; **shutdown-дренаж** (наполнить каналы → SIGTERM → всё дошло) | xUnit, `FakeTimeProvider`, in-memory `ITickSink` |
| **Concurrency** | parallel-нагрузка на дедуп/роутер; нет ArrayPool double-return | xUnit + стресс |
| **Integration** | COPY-путь в реальный Postgres; reconnect при обрыве; per-exchange форматы; миграции на старте | Testcontainers(Postgres) + MockExchange |
| **Load** | 100/с (спека) и burst; gen2/LOH ≈ 0; gap in/written | `dotnet-counters`, MockExchange-генератор |
| **Architecture** | `Domain`/`Application` без зависимости на `Infrastructure.*` | NetArchTest/кастомный |

Ключевые регрессионные тесты на каждую из 10 проблем (см. §15) — доказательство, что эталон их закрыл.

---

## 14. Конфигурация

`appsettings.json` + env. Коннекторы — типизированный список (`Exchanges[]`: `Name`, `Url` (wss),
`Format`, `Enabled`, `Symbols[]`), регистрируются **только enabled** (фикс №9: debug-коннектор есть
только в Development-профиле). Тюнинг-ручки: `Pipeline:ShardCount` (P), `Pipeline:WriterCount` (K),
`Pipeline:IngestChannelCapacity`, `Pipeline:BatchChannelCapacity`, `Pipeline:BatchMaxSize` (N),
`Pipeline:BatchMaxDelayMs` (T), `Shutdown:DrainTimeoutSeconds` (+ `HostOptions.ShutdownTimeout` >
этого). Все секреты — через env/user-secrets с `ValidateOnStart`.

---

## 15. Маппинг: 10 проблем ученика → как эталон их закрывает

| № | Проблема ученика | Решение в эталоне | Где |
|---|---|---|---|
| 1 | Гонка `TryAdd`+`Interlocked.Exchange` | single-writer дедуп (`SingleReader=true`), ноль локов — гонка невозможна by design | §5.2 |
| 2 | 32-битный хеш-ключ → ложные дубли | точный `TickKey` + тайбрейкер `SourceId`/фолбэк-счётчик | §6.2 |
| 3 | Переполнение `int` → негативный индекс | стабильный хеш, маскирование в неотрицательное; индексы шардов без wrap | §8 |
| 4 | Общий `DbContext` / captive dependency | connection-per-writer (`NpgsqlConnection`), нет общего соединения для захвата; EF только схема | §3, §5.1 |
| 5 | Shutdown теряет данные | двухфазный дренаж, отдельный drain-token, маленькие bounded-буферы | §5.4 |
| 6 | Polly ловит OCE | Polly v8 (дефолт исключает OCE); явно не добавляем catch-all | §2, §5.4 |
| 7 | Рассинхрон метрик | консистентный снимок всех счётчиков вместе; монотонные + дельты | §12 |
| 8 | Фикс-буфер 16 КБ | растущий `ArrayPool`-буфер до `MaxMessageBytes` + abort/reconnect | §11 |
| 9 | Debug-коннектор в проде | регистрация enabled-коннекторов из конфига; wss-валидация на старте | §11, §14 |
| 10 | sync `EnsureCreated()` в конструкторе | миграции отдельным `IHostedService` на старте через `IDbContextFactory` | §6.4 |

---

## 16. Фазы реализации (предварительно — детализируем в плане)

1. **Скелет solution + Domain** → verify: `dotnet build` зелёный; архитектурный тест проходит.
2. **MockExchange** (3 формата) → verify: руками подключиться wscat, видеть 3 формата.
3. **Processing core** (Channels, single-writer дедуп, батчер, shutdown-координатор) → verify:
   unit-тесты дедуп-гонки 10000×, батчер N-или-T, shutdown-дренаж — зелёные.
4. **WebSockets-коннекторы** (parse+normalize+validate+reconnect, растущий буфер) → verify:
   integration reconnect + per-format парсинг.
5. **Persistence.Postgres** (COPY-sink, миграции, схема) → verify: Testcontainers COPY-путь + миграции.
6. **Host** (DI, конфиг, метрики, Serilog, оркестрация) → verify: end-to-end MockExchange→Postgres,
   метрики в логах, gap in/written ≈ 0.
7. **Load + наблюдаемость** → verify: 100/с + burst, gen2/LOH ≈ 0 в `dotnet-counters`.
8. **Регрессии на 10 проблем** + README с защитой решений (No Vibecoding) → verify: все тесты §15.

---

## 17. Non-goals (явно)
- Брокер (Kafka/Redpanda/JetStream), Redis-дедуп, координатор/leader-election — **не** для теста (§10).
- Tier-1 spool / Tier-2 durability — документированный путь, не код.
- ClickHouse/Timescale-sink — демонстрируем seam (`ITickSink`), не реализуем.
- Аналитика/агрегации поверх raw-тиков — вне ТЗ (только raw storage).
- Глобальный порядок тиков между символами — намеренно не гарантируем (только per-symbol FIFO).

---

## 18. Заявленные гарантии (одним абзацем — для защиты на ревью)
Система принимает потоки 2–3 бирж разного формата, нормализует и дедуплицирует их с **точным**
составным ключом (без ложных схлопываний), пишет raw-тики в Postgres через binary COPY с
connection-per-writer. Конкурентность корректна **по конструкции**: single-writer дедуп (нет гонок),
async-коннекторы (нет заблокированных потоков), bounded-каналы (ограниченная память, явный backpressure).
Завершение — двухфазный дренаж без потери буферизованных данных при штатном SIGTERM. Честная граница:
at-most-once при `kill -9`, exactly-once-as-stored через идемпотентный ключ, exactly-once end-to-end
невозможно (WS без replay). Расширяемость: новая биржа = 1 класс + конфиг; смена БД = 1 строка DI.
Масштабирование за 100/с — шардинг по символу (дедуп остаётся локальным), документированный путь до
мульти-инстанса и брокера. Все 10 проблем эталонного код-ревью закрыты by design (§15).

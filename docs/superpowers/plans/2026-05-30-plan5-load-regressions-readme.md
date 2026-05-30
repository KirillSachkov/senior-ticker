# План 5 — нагрузка, регрессии на 10 проблем, README-защита, supply-chain (финал)

- **Дата:** 2026-05-30
- **Спека:** §7 (пропускная способность/память), §11 (supply-chain, least-privilege), §13 (load+regression),
  §15 (10 проблем), §16 фазы 7–8.
- **Предшественники:** Планы 1–4 (Domain/Processing/WebSockets/Persistence/Host — всё влито, 122 теста).
- **Цель:** довести эталон до «идеального состояния» для видео-разбора: доказать перф-заявления
  нагрузкой, дать **явный proof-артефакт** «все 10 проблем закрыты», написать README с защитой каждого
  решения (No Vibecoding), закрыть supply-chain и least-privilege.

---

## 0. Тезис фазы
Код готов и зелён; План 5 — **доказательная база и упаковка**. Ничего нового в горячем пути (кроме
возможной перф-настройки, выявленной нагрузкой); фокус — тесты-доказательства, README, supply-chain.

---

## 1. Нагрузка и аллокации (§7) — `tests/SeniorTicker.Processing.Tests/LoadTests.cs`
- **`Sustained_high_throughput_no_loss_and_low_gen2`:** прогнать ~100k уникальных тиков через
  `TickPipeline` со счётным sink'ом (без per-tick аллокаций), замерить `GC.CollectionCount(2)` до/после.
  Assert: (1) `written == ingested` (ноль потери), (2) дельта gen2 мала (struct `Tick` → ноль per-item
  heap; цель §7.3 «gen2/LOH ≈ 0»). Лог `Unsafe.SizeOf<Tick>()` и размера батч-массива — проверить
  границу LOH 85 000 байт (§7.2). **Если default `BatchMaxSize` даёт массив > LOH — это находка:**
  подкрутить дефолт так, чтобы `BatchMaxSize × sizeof(Tick) < 85_000` (или задокументировать), и
  доказать тестом, что gen2 не растёт.
- **`Burst_is_absorbed_by_backpressure`:** `GatedTickSink` (БД «висит»), залить burst > ёмкости
  каналов; assert — не OOM/не исключение (bounded-каналы держат), затем `Open()` → всё дренажится без
  потери. Доказывает §5.3 (backpressure = константная память, независимая от входной скорости).

## 2. Регрессии на 10 проблем (§13, §15) — явный proof-артефакт
- **`tests/SeniorTicker.Host.Tests/TenProblemsRegressionTests.cs`** — консолидированный набор, по одному
  чётко-помеченному `[Fact]` на проблему, юнит-уровень (без Docker): **#1** гонка дедупа (single-writer,
  high-concurrency → детерминированный дедуп), **#2** точный ключ (два разных тика в 1 мс не схлопнуты),
  **#3** нет отрицательного индекса (FNV-маска по враждебным символам → все в [0,P)), **#5** дренаж без
  потери, **#6** Polly не ретраит OCE, **#7** консистентный снимок метрик, **#8** растущий буфер +
  cap (мини-fake socket), **#9** wss-гейт валит публичный ws. Заголовок файла + README мапят **#4**
  (connection-per-writer) и **#10** (миграции через factory) на их канонические тесты в
  `Persistence.Tests` (Testcontainers — против реального Postgres).
- README §«10 проблем закрыты» — таблица problem → решение → имя теста → файл (master-индекс).

## 3. README.md (корень) — защита каждого решения (No Vibecoding)
Разделы: (1) что/зачем + тезис намеренно заниженной нагрузки; (2) quick start (MockExchange + Host,
env-секреты, Development-профиль); (3) архитектура (граф проектов, spine потока, модель конкурентности);
(4) **таблица 10 проблем** → как закрыта + тест; (5) конкурентность и двухфазный shutdown; (6) безопасность
(3 кольца); (7) семантика доставки (at-most-once при kill -9, exactly-once-as-stored, честные границы);
(8) перф и growth ladder; (9) конфигурация (все ручки + секреты); (10) тестирование (уровни, нужен Docker
для integration); (11) supply-chain + least-privilege БД; (12) non-goals. Источник — спека (не выдумывать).

## 4. Supply-chain (§11)
- `RestorePackagesWithLockFile=true` в `Directory.Build.props` → `dotnet restore` генерит
  `packages.lock.json` на проект; коммитим их. CI: `dotnet restore --locked-mode` (воспроизводимость,
  защита от dependency-confusion). README документирует `dotnet list package --vulnerable`.

## 5. least-privilege Postgres (§11) — `docs/postgres-least-privilege.sql`
Две роли: `ticker_migrator` (деплой-тайм, DDL/CREATE — гоняет EF-миграции) и `ticker_runtime`
(рантайм: `INSERT, SELECT` на `ticks`, `USAGE` на схему — БЕЗ `DELETE/UPDATE/TRUNCATE/DDL`; COPY-in
требует INSERT). Документ + README-врезка (в Testcontainers не применяем — там superuser; это
prod-guidance, как и мульти-инстанс).

---

## 6. Верификация (goal-driven)
1. `dotnet build` 0 warnings; нагрузочные + регрессионные + существующие — все зелёные. → verify.
2. Нагрузка: `written == ingested`, gen2-дельта мала, burst дренажится. → verify (если gen2 высок —
   фикс батч-дефолта под LOH, перепрогон). → verify.
3. 10 регрессий зелёные; README-таблица ссылается на реальные имена тестов. → verify (grep).
4. `dotnet restore --locked-mode` проходит на сгенерированных lock-файлах. → verify.
5. **Финальный whole-project adversarial-ревью** (Workflow: корректность/конкурентность/безопасность/
   доказательность 10 проблем/качество тестов/точность README/гигиена сборки/спек-конформанс →
   скептик-верификация → синтез). Все подтверждённые — починить. → verify «идеальное состояние».

## 7. Чего НЕ делаем
- Реальный `dotnet-counters`-прогон в CI (внешний инструмент) — заменяем на `GC.CollectionCount`-прокси
  в тесте + README-инструкцию для ручного прогона.
- Применение ролей в Testcontainers (superuser) — это prod-guidance.
- Брокер/мульти-инстанс/Tier-1 spool — документированные non-goals (§17 спеки).

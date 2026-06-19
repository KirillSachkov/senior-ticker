# Варианты решения StreamTicker (карта для видео)

Одна задача — четыре ступени, от наивной к проверяемой. В **коде** это две части: папка
наивного антипримера и одно боевое решение, которое покрывает базовый и продвинутый режимы
**конфигом** (`Pipeline:Mode`, `Pipeline:ShardCount`, выбор партиционера) — без форков.

## v0 — Наивный вариант (антипример)

Общее состояние без владельца, запись по одному тику, остановка одним токеном, без очередей.

- `src/SeniorTicker.Processing/Naive/NaiveDeduplicator.cs` — общий словарь + кольцо + индекс (гонка, коллизия, потеря SourceId)
- `src/SeniorTicker.Processing/Naive/NaiveTickProcessor.cs` — обработка на потоке-приёмнике, запись по тику
- `src/SeniorTicker.Processing/Naive/NaiveBufferingProcessor.cs` — остановка одним токеном (теряет буфер)
- `src/SeniorTicker.Processing/Naive/NaivePipeline.cs` — то же целиком как `ITickPipeline` (для запуска через Host)
- `src/SeniorTicker.Infrastructure.Persistence.Postgres/NaiveDbContextSink.cs` — общий `DbContext`, `SaveChanges` на тик
- **Запуск:** `Pipeline:Mode=Naive`. **Падающие тесты-доказательства:** `tests/SeniorTicker.Processing.Tests/SimpleVariantTests.cs`

## v1 — Базовый: Channels, один шард

Корректно для нагрузки тестового: ограниченные очереди (backpressure), один владелец дедупа
(без локов), батч + binary `COPY`, двухфазная остановка.

- `src/SeniorTicker.Processing/TickPipeline.cs` — конвейер (input → router → шард → батчи → writers)
- `src/SeniorTicker.Processing/SlidingWindowDeduplicator.cs` — окно дедупа (single-writer, без локов)
- `src/SeniorTicker.Infrastructure.Persistence.Postgres/CopyTickSink.cs` — соединение-на-пачку + binary COPY
- `src/SeniorTicker.Host/Pipeline/PipelineHostedService.cs` — двухфазная остановка (Complete → дренаж)
- **Запуск:** `Pipeline:Mode=Channels`, `Pipeline:ShardCount=1` (по умолчанию)

## v2 — Шардирование по Symbol

Тот же код, больше шардов: дедуп параллельный, но локальный (один владелец на инструмент).

- `src/SeniorTicker.Processing/SymbolShardPartitioner.cs` + `src/SeniorTicker.Domain/StableHash.cs`
- **Запуск:** `Pipeline:ShardCount=N` (партиционер по умолчанию — по `Symbol`)

## v3 — Горячий тикер: шард по TickKey

Когда один инструмент забивает поток и шардирование по `Symbol` не помогает. Тот же код, другой партиционер.

- `src/SeniorTicker.Processing/DedupKeyShardPartitioner.cs`
- Демо сравнения режимов под нагрузкой: `src/SeniorTicker.DemoHost/` + `frontend/senior-ticker-demo/`

## Лестница ↔ акты дека

| Акт | Ступень | Главное |
|-----|---------|---------|
| 1 | v0 наивный | ломаем по шагам: дедуп, запись, остановка, «без очередей» |
| 2 | v1 базовый | чиним по концепциям: очередь+backpressure, один владелец, батч+COPY, двухфазный стоп |
| 3 | v2 / v3 | масштаб: шарды по Symbol, горячий тикер по TickKey + доказательство тестами |

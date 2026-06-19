# Два решения StreamTicker (для разбора в видео)

Решение продублировано на две папки — так удобно показывать код «по папкам».
Общий нижний слой (Domain, Application, MockExchange, WebSockets) в обеих папках дублируется — это
сознательно, для обучения. Корневые `Directory.Build.props` / `Directory.Packages.props` / `global.json`
наследуются обеими (одни версии пакетов, без дублей конфигурации).

## `advanced/` — хорошее решение

Channels + шарды + binary COPY + двухфазная остановка. Запускается в режиме `Pipeline:Mode=Channels`
(по умолчанию). Все тесты зелёные.

- `advanced/SeniorTicker.sln`
- Дедупликация (один владелец, без локов): `advanced/src/SeniorTicker.Processing/SlidingWindowDeduplicator.cs`
- Конвейер: `advanced/src/SeniorTicker.Processing/TickPipeline.cs`
- Запись (соединение-на-пачку + COPY): `advanced/src/SeniorTicker.Infrastructure.Persistence.Postgres/CopyTickSink.cs`
- Двухфазная остановка: `advanced/src/SeniorTicker.Host/Pipeline/PipelineHostedService.cs`
- Шардирование: `SymbolShardPartitioner.cs` (по `Symbol`) и `DedupKeyShardPartitioner.cs` (горячий тикер, по `TickKey`) — переключаются конфигом `Pipeline:ShardCount` / партиционером.
- Демо сравнения режимов: `advanced/src/SeniorTicker.DemoHost/` + `frontend/senior-ticker-demo/`

## `naive/` — наивное решение (антипример)

Обработка на потоке-источнике, общий дедуп, запись по тику, остановка одним токеном, без очередей.
Запускается в режиме `Pipeline:Mode=Naive` (по умолчанию в `naive/`). Демо-тесты намеренно красные.

- `naive/SeniorTicker.Naive.sln`
- Наивный дедуп (общий словарь + кольцо, 32-битный ключ): `naive/src/SeniorTicker.Processing/Naive/NaiveDeduplicator.cs`
- Наивный конвейер (общий дедуп, запись по тику, безлимитный вход): `naive/src/SeniorTicker.Processing/Naive/NaivePipeline.cs`
- Наивная остановка одним токеном (теряет буфер): `naive/src/SeniorTicker.Processing/Naive/NaiveBufferingProcessor.cs`
- Наивная запись (общий `DbContext`, `SaveChanges` на тик): `naive/src/SeniorTicker.Infrastructure.Persistence.Postgres/NaiveDbContextSink.cs`
- Падающие демо-тесты: `naive/tests/SeniorTicker.Processing.Tests/SimpleVariantTests.cs`
  - `dotnet test naive/SeniorTicker.Naive.sln` → красные: 1000→20 (дедуп), коллапс SourceId, ArgumentException (запись), 100→0 (остановка).

## Лестница ↔ акты дека

| Акт | Папка | Главное |
|-----|-------|---------|
| 1 | `naive/` | ломаем по шагам: дедупликация, запись, остановка, «без очередей» |
| 2 | `advanced/` | чиним по концепциям: очередь+backpressure, один владелец, батч+COPY, двухфазная остановка |
| 3 | `advanced/` | масштаб: шарды по `Symbol`, горячий тикер по `TickKey` + доказательство тестами |

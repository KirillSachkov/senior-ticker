# Решение StreamTicker (для разбора в видео)

Одно решение в папке `advanced/`. Корневые `Directory.Build.props` / `Directory.Packages.props` /
`global.json` задают единые версии пакетов для всех проектов.

## `advanced/` — конвейер обработки тиков

Channels + шарды + binary COPY + двухфазная остановка. Запускается в режиме `Pipeline:Mode=Channels`
(по умолчанию). Все тесты зелёные.

- `advanced/SeniorTicker.sln`
- Дедупликация (один владелец, без локов): `advanced/src/SeniorTicker.Processing/SlidingWindowDeduplicator.cs`
- Конвейер: `advanced/src/SeniorTicker.Processing/TickPipeline.cs`
- Запись (соединение-на-пачку + COPY): `advanced/src/SeniorTicker.Infrastructure.Persistence.Postgres/CopyTickSink.cs`
- Двухфазная остановка: `advanced/src/SeniorTicker.Host/Pipeline/PipelineHostedService.cs`
- Шардирование: `SymbolShardPartitioner.cs` (по `Symbol`) и `DedupKeyShardPartitioner.cs` (горячий тикер, по `TickKey`) — переключаются конфигом `Pipeline:ShardCount` / партиционером.

## Демо: живой дашборд конвейера

`advanced/src/SeniorTicker.DemoHost/` + `frontend/senior-ticker-demo/` — live-дашборд одного
конвейера под нагрузкой. Параметры нагрузки (тиков/с, доля горячего символа, дубли, шарды,
писатели, размер батча, задержка БД) задаются с фронтенда; снимок метрик стримится по SSE.

Метрики на карте конвейера:

- принято / дедуплицировано / записано / в БД;
- глубина очередей (вход + батч-канал + шарды) — backpressure держит память под нагрузкой;
- пропускная способность (получено/с, записано/с);
- распределение входа по шардам и глубина очередей шардов.

API: `GET /api/demo/snapshot`, `GET /api/demo/events` (SSE), `POST /api/demo/start|stop|reset`.

## Концепции, которые показывает демо

| Концепция | Где в коде |
|-----------|------------|
| Очередь + backpressure (ограниченные каналы) | `TickPipeline.cs`, `PipelineOptions` |
| Один владелец дедупликации (без локов) | `SlidingWindowDeduplicator.cs` |
| Батч + binary COPY | `CopyTickSink.cs` |
| Двухфазная остановка (дренаж без потерь) | `PipelineHostedService.cs` |
| Шарды по `Symbol` / горячий тикер по `TickKey` | `SymbolShardPartitioner.cs`, `DedupKeyShardPartitioner.cs` |

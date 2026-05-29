# SeniorTicker

Эталонное senior-решение тестового задания «система агрегации и обработки данных с биржевых
торгов»: сбор котировок с нескольких бирж по WebSocket, нормализация, дедупликация и хранение
raw-тиков в PostgreSQL в реальном времени.

> Стек: **.NET 10** · System.Threading.Channels · PostgreSQL (Npgsql binary COPY) · Polly v8 ·
> Serilog · xUnit + Testcontainers.

## Документы
- [Design / архитектура решения](docs/superpowers/specs/2026-05-29-senior-ticker-design.md) —
  полный разбор: декомпозиция, модель конкурентности, graceful shutdown, семантика доставки,
  точки роста, безопасность, маппинг «10 проблем код-ревью → решения».

Статус: проектирование завершено, реализация по фазам (см. §16 design-документа).

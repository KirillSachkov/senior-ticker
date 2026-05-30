-- Least-privilege роли для SeniorTicker (спека §11).
-- Принцип: рантайм-роль умеет ТОЛЬКО писать/читать тики; DDL и удаление — у отдельной деплой-роли.
-- В Testcontainers это не применяется (там superuser) — это prod-guidance, как и мульти-инстанс.
--
-- Применять под суперпользователем один раз на окружении. Пароли — из секрет-стора, не из файла.

-- ── 1. Деплой-роль: гоняет EF-миграции (DDL). Используется ТОЛЬКО на деплое, не рантаймом. ──────────
CREATE ROLE ticker_migrator LOGIN PASSWORD :'migrator_password';
GRANT CONNECT ON DATABASE seniorticker TO ticker_migrator;
GRANT CREATE, USAGE ON SCHEMA public TO ticker_migrator;   -- CREATE TABLE/INDEX, история миграций
-- (миграции создадут таблицу ticks и __EFMigrationsHistory под этой ролью)

-- ── 2. Рантайм-роль: least privilege — только запись/чтение тиков. ──────────────────────────────────
CREATE ROLE ticker_runtime LOGIN PASSWORD :'runtime_password';
GRANT CONNECT ON DATABASE seniorticker TO ticker_runtime;
GRANT USAGE ON SCHEMA public TO ticker_runtime;            -- НЕ CREATE → роль не может менять схему
GRANT INSERT, SELECT ON TABLE ticks TO ticker_runtime;     -- COPY ... FROM STDIN требует INSERT; SELECT — чтения/метрики
GRANT USAGE, SELECT ON SEQUENCE ticks_id_seq TO ticker_runtime;  -- identity-суррогат id
-- ЯВНО не выдаём: DELETE, UPDATE, TRUNCATE, DROP, и любые права на другие таблицы/схемы.

-- ── 3. Жёсткая отмена «всего по умолчанию» (defense-in-depth). ──────────────────────────────────────
REVOKE ALL ON DATABASE seniorticker FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Приложение (Host) подключается строкой ticker_runtime; миграции деплой-скриптом — ticker_migrator.
-- MaxPoolSize строки рантайм-роли = K writer-воркеров + headroom (см. Postgres:MaxWriterConnections).

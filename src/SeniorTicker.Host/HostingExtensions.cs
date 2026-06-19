using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Serilog;
using SeniorTicker.Application;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Host.Ingestion;
using SeniorTicker.Host.Observability;
using SeniorTicker.Host.Persistence;
using SeniorTicker.Host.Pipeline;
using SeniorTicker.Infrastructure.Persistence.Postgres;
using SeniorTicker.Processing;
using SeniorTicker.Processing.Naive;

namespace SeniorTicker.Host;

/// <summary>
/// Composition root: единственное место, где сходятся конфиг, секреты, Serilog, DI и порядок
/// хостед-сервисов. Порядок регистрации хостед-сервисов = хореография двухфазного дренажа (§5.4):
/// DbInit (миграции до writers, #10) → Metrics → Pipeline → Connectors (стоп ПЕРВЫМ, #5/#6).
/// </summary>
public static class HostingExtensions
{
    public static HostApplicationBuilder AddSeniorTicker(this HostApplicationBuilder builder)
    {
        // dev-секреты (§11): appsettings хранит имена; ConnectionStrings:Postgres — в env (prod) / user-secrets (dev).
        // User-secrets ТОЛЬКО в Development и ПЕРЕД env (re-add env), иначе устаревший secrets.json молча перебил
        // бы ConnectionStrings__Postgres из env (Host.CreateApplicationBuilder сам user-secrets не добавляет).
        if (builder.Environment.IsDevelopment())
        {
            builder.Configuration.AddUserSecrets(Assembly.GetExecutingAssembly(), optional: true);
            builder.Configuration.AddEnvironmentVariables();
        }

        // Serilog: уровни из конфига, консольный sink в коде (без двойного sink через config-discovery).
        builder.Services.AddSerilog((sp, lc) => lc
            .ReadFrom.Configuration(builder.Configuration)
            .ReadFrom.Services(sp)
            .Enrich.FromLogContext()
            .WriteTo.Console());

        // Конфиг + гейт старта (#9 wss-валидация → fail boot).
        builder.Services.AddOptions<SeniorTickerOptions>()
            .Bind(builder.Configuration)
            .ValidateOnStart();
        builder.Services.AddSingleton<IValidateOptions<SeniorTickerOptions>, SeniorTickerOptionsValidator>();

        // Двухфазный дренаж требует ShutdownTimeout > drain и СТРОГО последовательного LIFO-останова.
        // clamp — defense-in-depth: валидатор завалит <=0 на ValidateOnStart, но HostOptions считается
        // на build, и misconfig не должен схлопнуть бюджет хоста до 5с раньше срабатывания валидатора.
        var drainSeconds = Math.Max(1, builder.Configuration.GetValue<int?>("Shutdown:DrainTimeoutSeconds") ?? 30);
        builder.Services.Configure<HostOptions>(o =>
        {
            o.ShutdownTimeout = TimeSpan.FromSeconds(drainSeconds + 5);
            o.ServicesStartConcurrently = false;
            o.ServicesStopConcurrently = false;
        });

        builder.Services.AddSingleton(TimeProvider.System);

        // Производные значения из конфига → как plain-singletons (downstream не зависит от IOptions).
        builder.Services.AddSingleton(sp =>
            sp.GetRequiredService<IOptions<SeniorTickerOptions>>().Value.Pipeline.ToOptions());
        builder.Services.AddSingleton(sp =>
            sp.GetRequiredService<IOptions<SeniorTickerOptions>>().Value.Shutdown);
        builder.Services.AddSingleton(sp =>
            sp.GetRequiredService<IOptions<SeniorTickerOptions>>().Value.Metrics);

        // Метрики (#7): один экземпляр и как concrete (snapshot для фонового сервиса), и как порт.
        builder.Services.AddSingleton<MetricsSink>();
        builder.Services.AddSingleton<IMetricsSink>(sp => sp.GetRequiredService<MetricsSink>());

        // Конвейер: боевой TickPipeline или учебный NaivePipeline (Pipeline:Mode=Naive) — оба через ITickPipeline.
        var naivePipeline = string.Equals(
            builder.Configuration.GetValue<string>("Pipeline:Mode"), "Naive", StringComparison.OrdinalIgnoreCase);
        if (naivePipeline)
            builder.Services.AddSingleton<ITickPipeline>(sp =>
                new NaivePipeline(sp.GetRequiredService<ITickSink>(), sp.GetRequiredService<IMetricsSink>()));
        else
            builder.Services.AddSingleton<ITickPipeline, TickPipeline>();
        builder.Services.AddSingleton<ITickIngestor, ChannelTickIngestor>();
        builder.Services.AddSingleton<ConnectorFactory>();

        // Persistence: строка подключения — секрет (env/user-secrets), отсутствие = fail boot (§11).
        var connectionString = builder.Configuration.GetConnectionString("Postgres");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Missing connection string 'ConnectionStrings:Postgres'. Set it via environment " +
                "('ConnectionStrings__Postgres') or user-secrets — never in appsettings (§11).");
        }
        var maxWriters = builder.Configuration.GetValue<int?>("Postgres:MaxWriterConnections") ?? 8;
        var writerCount = builder.Configuration.GetValue<int?>("Pipeline:WriterCount") ?? 2;
        // §11 invariant «MaxPoolSize = K + headroom»: K writer-воркеров и EF/migration-контекст делят
        // один NpgsqlDataSource. Без запаса K-й writer и миграции конкурируют за пул → connection-exhaustion
        // под нагрузкой. Гейтим на старте (fail boot), а не оставляем инвариант декларативным.
        if (maxWriters < writerCount + 1)
        {
            throw new InvalidOperationException(
                $"Postgres:MaxWriterConnections ({maxWriters}) must be >= Pipeline:WriterCount ({writerCount}) + 1 " +
                "headroom for the EF/migration connection (§11 anti-connection-exhaustion).");
        }
        builder.Services.AddPostgresPersistence(new PostgresOptions
        {
            ConnectionString = connectionString,
            MaxWriterConnections = maxWriters,
        });

        // Naive-режим: подменяем боевой COPY-sink на общий DbContext + SaveChanges-на-тик (антипример записи).
        if (naivePipeline)
            builder.Services.AddSingleton<ITickSink, NaiveDbContextSink>();

        // Хостед-сервисы — ПОРЯДОК = хореография §5.4 (старт сверху-вниз, останов снизу-вверх).
        builder.Services.AddHostedService<DatabaseInitializerHostedService>();   // (1) миграции до writers (#10)
        builder.Services.AddHostedService<MetricsBackgroundService>();           // (2) метрики (живут до конца дренажа)
        builder.Services.AddSingleton<PipelineHostedService>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<PipelineHostedService>()); // (3) конвейер
        builder.Services.AddHostedService<ConnectorHostedService>();             // (4) коннекторы → стоп ПЕРВЫМ (#5/#6)

        return builder;
    }
}

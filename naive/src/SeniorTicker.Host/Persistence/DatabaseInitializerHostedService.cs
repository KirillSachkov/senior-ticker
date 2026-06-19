using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SeniorTicker.Infrastructure.Persistence.Postgres;

namespace SeniorTicker.Host.Persistence;

/// <summary>
/// Фикс #10: применяет миграции на старте через <see cref="DatabaseInitializer"/> (MigrateAsync via
/// IDbContextFactory, План 3) — НЕ <c>EnsureCreated()</c> в конструкторе репозитория. Регистрируется
/// ПЕРВЫМ → <c>StartAsync</c> дожидается миграций до старта writers (порядок старта = порядок
/// регистрации при <c>ServicesStartConcurrently=false</c>). <c>StopAsync</c> — noop.
/// </summary>
public sealed class DatabaseInitializerHostedService(
    DatabaseInitializer initializer,
    ILogger<DatabaseInitializerHostedService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Applying database migrations…");
        await initializer.InitializeAsync(cancellationToken);
        logger.LogInformation("Database schema ready.");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

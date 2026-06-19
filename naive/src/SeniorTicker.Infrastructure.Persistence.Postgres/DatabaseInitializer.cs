using Microsoft.EntityFrameworkCore;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>
/// Применяет миграции на старте через IDbContextFactory (фикс #10: НЕ EnsureCreated() в конструкторе
/// репозитория — это игнорировало миграции и было sync-I/O не на месте). Вызывается Host-ом до старта writers.
/// </summary>
public sealed class DatabaseInitializer(IDbContextFactory<TickDbContext> factory)
{
    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var db = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        await db.Database.MigrateAsync(ct).ConfigureAwait(false);
    }
}

using Microsoft.EntityFrameworkCore;
using Npgsql;
using SeniorTicker.Infrastructure.Persistence.Postgres;

namespace SeniorTicker.Persistence.Tests;

[Collection("postgres")]
public class DatabaseInitializerTests(PostgresFixture fx)
{
    // Минимальная IDbContextFactory без DI — CreateDbContextAsync берётся из дефолтной реализации интерфейса.
    private sealed class Factory(DbContextOptions<TickDbContext> options) : IDbContextFactory<TickDbContext>
    {
        public TickDbContext CreateDbContext() => new(options);
    }

    [Fact]
    public async Task Initialize_applies_migrations_via_factory_and_is_idempotent()
    {
        // Фикс #10: миграции через IDbContextFactory.MigrateAsync на старте, НЕ EnsureCreated() в конструкторе.
        var options = new DbContextOptionsBuilder<TickDbContext>().UseNpgsql(fx.ConnectionString).Options;
        var initializer = new DatabaseInitializer(new Factory(options));

        // повторный вызов идемпотентен (история миграций уже применена → no-op, без исключения)
        await initializer.InitializeAsync(CancellationToken.None);
        await initializer.InitializeAsync(CancellationToken.None);

        await using var conn = await fx.DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("SELECT to_regclass('public.ticks')::text", conn);
        Assert.Equal("ticks", await cmd.ExecuteScalarAsync() as string);
    }
}

using Microsoft.EntityFrameworkCore;
using Npgsql;
using SeniorTicker.Infrastructure.Persistence.Postgres;
using Testcontainers.PostgreSql;
using Xunit;

namespace SeniorTicker.Persistence.Tests;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
        .Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;
    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        // применяем миграции (как сделает DatabaseInitializer в Host'е)
        var options = new DbContextOptionsBuilder<TickDbContext>().UseNpgsql(ConnectionString).Options;
        await using (var db = new TickDbContext(options))
            await db.Database.MigrateAsync();
        DataSource = new NpgsqlDataSourceBuilder(ConnectionString).Build();
    }

    public async Task DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }

    public async Task TruncateAsync()
    {
        await using var conn = await DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("TRUNCATE ticks RESTART IDENTITY", conn);
        await cmd.ExecuteNonQueryAsync();
    }
}

[CollectionDefinition("postgres")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;

using Npgsql;
using Testcontainers.PostgreSql;

namespace SeniorTicker.DemoHost.Tests;

public sealed class DemoPostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
        .Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("demo-postgres")]
public sealed class DemoPostgresCollection : ICollectionFixture<DemoPostgresFixture>;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

public sealed class PostgresOptions
{
    public required string ConnectionString { get; init; }
    /// <summary>Верхняя граница соединений для записи (NpgsqlDataSource MaxPoolSize headroom).</summary>
    public int MaxWriterConnections { get; init; } = 8;
}

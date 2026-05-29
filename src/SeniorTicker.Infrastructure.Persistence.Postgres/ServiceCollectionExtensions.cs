using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SeniorTicker.Application;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует: NpgsqlDataSource (singleton, потокобезопасный пул) для горячей записи COPY;
    /// IDbContextFactory (схема/чтения); ITickSink → CopyTickSink; DatabaseInitializer.
    /// </summary>
    public static IServiceCollection AddPostgresPersistence(this IServiceCollection services, PostgresOptions options)
    {
        var dataSource = new NpgsqlDataSourceBuilder(options.ConnectionString)
        {
            // headroom: writers + EF; защита max_connections (§11)
        }.Build();
        services.AddSingleton(dataSource);

        services.AddDbContextFactory<TickDbContext>(o => o.UseNpgsql(dataSource));
        services.AddSingleton(options);
        services.AddSingleton<DatabaseInitializer>();
        services.AddSingleton<ITickSink, CopyTickSink>();
        return services;
    }
}

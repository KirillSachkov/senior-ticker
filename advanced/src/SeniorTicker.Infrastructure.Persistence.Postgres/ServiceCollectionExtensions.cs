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
        // Регистрируем через factory (не instance-overload), чтобы DI ВЛАДЕЛ NpgsqlDataSource и
        // вызвал DisposeAsync на остановке хоста — иначе пул физических соединений не закрывается gracefully.
        services.AddSingleton<NpgsqlDataSource>(_ =>
        {
            var dataSourceBuilder = new NpgsqlDataSourceBuilder(options.ConnectionString);
            // §11: ограничиваем пул (K writer-воркеров + headroom для EF/миграций) против connection-exhaustion.
            dataSourceBuilder.ConnectionStringBuilder.MaxPoolSize = options.MaxWriterConnections;
            return dataSourceBuilder.Build();
        });

        services.AddDbContextFactory<TickDbContext>((sp, o) => o.UseNpgsql(sp.GetRequiredService<NpgsqlDataSource>()));
        services.AddSingleton(options);
        services.AddSingleton<DatabaseInitializer>();
        services.AddSingleton<ITickSink, CopyTickSink>();
        return services;
    }
}

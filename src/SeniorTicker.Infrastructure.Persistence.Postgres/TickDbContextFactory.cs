using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

/// <summary>Только для `dotnet ef` (design-time). Runtime использует AddPostgresPersistence.</summary>
public sealed class TickDbContextFactory : IDesignTimeDbContextFactory<TickDbContext>
{
    public TickDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<TickDbContext>()
            .UseNpgsql("Host=localhost;Database=seniorticker;Username=postgres;Password=postgres")
            .Options;
        return new TickDbContext(options);
    }
}

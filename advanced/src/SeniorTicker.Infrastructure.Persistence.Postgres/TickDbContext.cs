using Microsoft.EntityFrameworkCore;

namespace SeniorTicker.Infrastructure.Persistence.Postgres;

public sealed class TickDbContext(DbContextOptions<TickDbContext> options) : DbContext(options)
{
    public DbSet<TickEntity> Ticks => Set<TickEntity>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        var e = b.Entity<TickEntity>();
        e.ToTable("ticks");
        e.Metadata.SetStorageParameter("fillfactor", 100); // строки не UPDATE-ятся
        e.HasKey(x => x.Id);
        e.Property(x => x.Id).UseIdentityByDefaultColumn();
        e.Property(x => x.Exchange).HasColumnName("exchange").HasConversion<short>();
        e.Property(x => x.Symbol).HasColumnName("symbol").HasMaxLength(32);
        e.Property(x => x.Price).HasColumnName("price").HasPrecision(38, 18);
        e.Property(x => x.Volume).HasColumnName("volume").HasPrecision(38, 18);
        e.Property(x => x.ExchangeTimestamp).HasColumnName("exchange_ts");
        e.Property(x => x.SourceId).HasColumnName("source_id");
        e.Property(x => x.IngestTimestamp).HasColumnName("ingest_ts");

        // UNIQUE backstop по точному составному ключу дедупликации (фикс класса #2 на уровне БД)
        e.HasIndex(x => new { x.Exchange, x.Symbol, x.ExchangeTimestamp, x.SourceId })
            .IsUnique()
            .HasDatabaseName("ux_ticks_dedup_key");

        // BRIN по времени: крошечный, дёшев на вставку, для range-сканов (§7)
        e.HasIndex(x => x.ExchangeTimestamp)
            .HasDatabaseName("ix_ticks_exchange_ts_brin")
            .HasMethod("brin");
    }
}

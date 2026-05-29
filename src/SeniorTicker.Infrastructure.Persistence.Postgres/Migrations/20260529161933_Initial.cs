using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace SeniorTicker.Infrastructure.Persistence.Postgres.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ticks",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    exchange = table.Column<short>(type: "smallint", nullable: false),
                    symbol = table.Column<string>(type: "character varying(32)", maxLength: 32, nullable: false),
                    price = table.Column<decimal>(type: "numeric(38,18)", precision: 38, scale: 18, nullable: false),
                    volume = table.Column<decimal>(type: "numeric(38,18)", precision: 38, scale: 18, nullable: false),
                    exchange_ts = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    source_id = table.Column<long>(type: "bigint", nullable: false),
                    ingest_ts = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ticks", x => x.Id);
                })
                .Annotation("Npgsql:StorageParameter:fillfactor", 100);

            migrationBuilder.CreateIndex(
                name: "ix_ticks_exchange_ts_brin",
                table: "ticks",
                column: "exchange_ts")
                .Annotation("Npgsql:IndexMethod", "brin");

            migrationBuilder.CreateIndex(
                name: "ux_ticks_dedup_key",
                table: "ticks",
                columns: new[] { "exchange", "symbol", "exchange_ts", "source_id" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ticks");
        }
    }
}

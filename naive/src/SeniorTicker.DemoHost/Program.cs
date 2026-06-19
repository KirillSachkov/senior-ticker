using System.Text.Json;
using Npgsql;
using SeniorTicker.DemoHost.Demo;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Port=5432;Database=senior_ticker;Username=postgres;Password=postgres";

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy => policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod());
});
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<DemoDatabase>();
builder.Services.AddSingleton<ComparativeDemoRunner>();

var app = builder.Build();
app.UseCors();

app.MapGet("/api/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/api/demo/snapshot", (ComparativeDemoRunner runner) => runner.Snapshot());

app.MapPost("/api/demo/start", async (DemoConfig config, ComparativeDemoRunner runner, CancellationToken ct) =>
{
    await runner.StartAsync(config, ct);
    return Results.Ok(runner.Snapshot());
});

app.MapPost("/api/demo/stop", async (ComparativeDemoRunner runner, CancellationToken ct) =>
{
    await runner.StopAsync(ct);
    return Results.Ok(runner.Snapshot());
});

app.MapPost("/api/demo/reset", async (ComparativeDemoRunner runner, CancellationToken ct) =>
{
    await runner.ResetAsync(ct);
    return Results.Ok(runner.Snapshot());
});

app.MapGet("/api/demo/events", async (HttpContext context, ComparativeDemoRunner runner) =>
{
    context.Response.Headers.CacheControl = "no-cache";
    context.Response.Headers.ContentType = "text/event-stream";

    while (!context.RequestAborted.IsCancellationRequested)
    {
        var json = JsonSerializer.Serialize(runner.Snapshot(), JsonSerializerOptions.Web);
        await context.Response.WriteAsync($"data: {json}\n\n", context.RequestAborted);
        await context.Response.Body.FlushAsync(context.RequestAborted);
        await Task.Delay(500, context.RequestAborted);
    }
});

await app.RunAsync();

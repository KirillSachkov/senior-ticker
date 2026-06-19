using Microsoft.Extensions.Hosting;
using Serilog;
using SeniorTicker.Host;

// Bootstrap-логгер ловит ошибки старта (конфиг/DI/валидация) до построения финального логгера.
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateBootstrapLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);
    builder.AddSeniorTicker();

    using var host = builder.Build();
    await host.RunAsync();
    return 0;
}
catch (Exception ex)
{
    Log.Fatal(ex, "Host terminated unexpectedly");
    return 1;
}
finally
{
    Log.CloseAndFlush();
}

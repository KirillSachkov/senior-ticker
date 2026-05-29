using System.Net.WebSockets;
using System.Text;

namespace SeniorTicker.MockExchange;

public static class MockExchangeApp
{
    public static WebApplication Build(WebApplicationBuilder builder)
    {
        var app = builder.Build();
        app.UseWebSockets();
        app.Map("/binance", (HttpContext ctx) => Stream(ctx, MockTickGenerator.Binance, "BTCUSDT"));
        app.Map("/kraken",  (HttpContext ctx) => Stream(ctx, MockTickGenerator.Kraken,  "BTC/USD"));
        app.Map("/csv",     (HttpContext ctx) => Stream(ctx, MockTickGenerator.Csv,     "ETHUSD"));
        return app;
    }

    private static async Task Stream(HttpContext ctx, Func<string, long, long, string> fmt, string symbol)
    {
        if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
        var dropAfter = int.TryParse(ctx.Request.Query["dropAfter"], out var d) ? d : int.MaxValue;
        using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
        var ct = ctx.RequestAborted;
        long id = 0;
        try
        {
            while (socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var payload = Encoding.UTF8.GetBytes(fmt(symbol, ++id, nowMs));
                await socket.SendAsync(payload, WebSocketMessageType.Text, endOfMessage: true, ct);
                if (id >= dropAfter)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "dropAfter", ct);
                    return;
                }
                await Task.Delay(20, ct); // ~50 сообщений/сек
            }
        }
        catch (OperationCanceledException) { /* клиент отключился */ }
        catch (WebSocketException) { /* соединение разорвано */ }
    }
}

namespace SeniorTicker.Infrastructure.WebSockets;

public sealed class WebSocketConnectorOptions
{
    public required string Name { get; init; }
    public required Uri Url { get; init; }
}

using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

public sealed class WebSocketConnectorOptions
{
    public required string Name { get; init; }
    public required Exchange Exchange { get; init; }
    public required Uri Url { get; init; }
    public int InitialReceiveBufferBytes { get; init; } = 4 * 1024;
    public int MaxMessageBytes { get; init; } = 256 * 1024;
    public TimeSpan ReconnectBaseDelay { get; init; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(30);
}

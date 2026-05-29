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

    /// <summary>Таймаут TCP/TLS/WS-хендшейка. Зависший connect → исключение → Polly reconnect (WS-2).</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Максимум тишины между сообщениями. Peer, который держит сокет открытым, отвечает на PING,
    /// но не шлёт данные (slowloris / half-open), иначе завесил бы receive-loop навсегда без исключения —
    /// Polly не переподключился бы. По истечении → исключение (не shutdown-OCE) → reconnect (WS-1).
    /// </summary>
    public TimeSpan ReceiveIdleTimeout { get; init; } = TimeSpan.FromSeconds(60);
}

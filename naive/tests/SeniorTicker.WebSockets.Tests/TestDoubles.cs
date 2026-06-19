using System.Collections.Concurrent;
using System.Net.WebSockets;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.WebSockets.Tests;

public sealed class CollectingIngestor : ITickIngestor
{
    private readonly ConcurrentQueue<Tick> _ticks = new();
    public ValueTask IngestAsync(Tick tick, CancellationToken ct)
    {
        _ticks.Enqueue(tick);
        return ValueTask.CompletedTask;
    }
    public IReadOnlyCollection<Tick> Ticks => _ticks.ToArray();
}

/// <summary>WebSocket-дублёр: отдаёт заранее заданные фрагменты (для теста растущего буфера).</summary>
public sealed class FakeWebSocket : WebSocket
{
    private readonly LinkedList<(byte[] Data, bool EndOfMessage, bool Close)> _frames;
    public FakeWebSocket(IEnumerable<(byte[] Data, bool EndOfMessage, bool Close)> frames)
        => _frames = new(frames);

    public override ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken ct)
    {
        if (_frames.Count == 0)
            return ValueTask.FromResult(new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true));
        var (data, eom, close) = _frames.First!.Value;
        _frames.RemoveFirst();
        if (close)
            return ValueTask.FromResult(new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true));
        var n = Math.Min(data.Length, buffer.Length);
        data.AsSpan(0, n).CopyTo(buffer.Span);
        if (n < data.Length) // фрагмент не влез целиком — вернуть остаток следующим фреймом (в начало очереди)
            _frames.AddFirst((data[n..], eom, false));
        var thisEom = n == data.Length && eom;
        return ValueTask.FromResult(new ValueWebSocketReceiveResult(n, WebSocketMessageType.Binary, thisEom));
    }

    public override WebSocketCloseStatus? CloseStatus => null;
    public override string? CloseStatusDescription => null;
    public override WebSocketState State => WebSocketState.Open;
    public override string? SubProtocol => null;
    public override void Abort() { }
    public override Task CloseAsync(WebSocketCloseStatus s, string? d, CancellationToken ct) => Task.CompletedTask;
    public override Task CloseOutputAsync(WebSocketCloseStatus s, string? d, CancellationToken ct) => Task.CompletedTask;
    public override void Dispose() { }
    public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken ct)
        => throw new NotSupportedException();
    public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType t, bool eom, CancellationToken ct)
        => Task.CompletedTask;
}

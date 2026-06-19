using System.Buffers;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Полученное WS-сообщение поверх арендованного из ArrayPool буфера. Span не пересекает await
/// (парсинг синхронный после receive). Dispose возвращает буфер в пул — НЕ использовать Span после Dispose.
/// </summary>
public readonly struct ReceivedMessage : IDisposable
{
    private readonly byte[]? _rented;
    private readonly int _length;

    public bool IsClosed { get; }

    private ReceivedMessage(byte[]? rented, int length, bool isClosed)
    {
        _rented = rented;
        _length = length;
        IsClosed = isClosed;
    }

    public static ReceivedMessage Message(byte[] rented, int length) => new(rented, length, false);
    public static readonly ReceivedMessage Closed = new(null, 0, true);

    public ReadOnlySpan<byte> Span => _rented is null ? default : _rented.AsSpan(0, _length);

    /// <summary>
    /// ОДНОРАЗОВЫЙ контракт: каждый Dispose возвращает буфер в пул. НЕ копировать значение struct и
    /// не звать Dispose дважды — это double-return одного и того же массива (порча пула). Guard-флага
    /// нет намеренно: readonly struct не может мутировать состояние.
    /// </summary>
    public void Dispose()
    {
        if (_rented is not null)
            ArrayPool<byte>.Shared.Return(_rented);
    }
}

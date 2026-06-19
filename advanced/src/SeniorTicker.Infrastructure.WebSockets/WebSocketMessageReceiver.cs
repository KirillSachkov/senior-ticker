using System.Buffers;
using System.Net.WebSockets;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Читает ОДНО полное WS-сообщение в растущий ArrayPool-буфер. Фикс #8: фиксированный буфер
/// молча терял/обрывал сообщения больше его размера; здесь буфер растёт по мере накопления
/// фрагментов до EndOfMessage, а превышение MaxMessageBytes — ЯВНОЕ исключение (а не молчаливое
/// усечение и не безграничный рост → защита от memory-DoS).
/// </summary>
public sealed class WebSocketMessageReceiver(int initialBufferBytes, int maxMessageBytes)
{
    public async ValueTask<ReceivedMessage> ReceiveAsync(WebSocket socket, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Min(initialBufferBytes, maxMessageBytes));
        var total = 0;
        try
        {
            while (true)
            {
                var capacity = Math.Min(buffer.Length, maxMessageBytes); // не пишем за лимит сообщения
                if (total == capacity)
                {
                    if (buffer.Length >= maxMessageBytes)
                        throw new InvalidOperationException(
                            $"WS message exceeds MaxMessageBytes={maxMessageBytes}");
                    var newSize = (int)Math.Min((long)buffer.Length * 2, maxMessageBytes); // long-каст: без int-overflow
                    var bigger = ArrayPool<byte>.Shared.Rent(newSize);
                    Array.Copy(buffer, bigger, total);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = bigger;
                    capacity = Math.Min(buffer.Length, maxMessageBytes);
                }

                var result = await socket.ReceiveAsync(buffer.AsMemory(total, capacity - total), ct);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    return ReceivedMessage.Closed;
                }

                if (result.Count == 0 && !result.EndOfMessage)
                    throw new InvalidOperationException("WS peer returned an empty non-final frame"); // защита от hot-spin
                total += result.Count;
                if (result.EndOfMessage)
                    return ReceivedMessage.Message(buffer, total); // владение буфером уходит в ReceivedMessage
            }
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }
}

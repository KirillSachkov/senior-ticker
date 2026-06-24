using System.Buffers;
using System.Net.WebSockets;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Читает ОДНО полное WS-сообщение в буфер, который при нехватке места растёт. Сообщение может прийти
/// несколькими кадрами (фрагментация), поэтому кадры склеиваются в один непрерывный буфер до EndOfMessage.
/// Фикс #8: фиксированный буфер молча резал крупные сообщения в битый JSON. Здесь буфер растёт до
/// MaxMessageBytes, а превышение это ЯВНОЕ исключение, не молчаливое усечение и не безграничный рост
/// (защита от memory-DoS). Буфер берётся из общего ArrayPool и туда же возвращается.
/// </summary>
public sealed class WebSocketMessageReceiver(int initialBufferBytes, int maxMessageBytes)
{
    public async ValueTask<ReceivedMessage> ReceiveAsync(WebSocket socket, CancellationToken ct)
    {
        // Арендуем стартовый буфер из общего пула. Math.Min: если потолок сообщения меньше стартового
        // размера, нет смысла брать больше потолка.
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Min(initialBufferBytes, maxMessageBytes));

        // total = сколько байт сообщения уже накоплено в buffer. Это и длина собранного, и позиция,
        // с которой дописываем следующий кадр.
        var total = 0;
        try
        {
            // Один проход цикла = один кадр. Крутимся, пока не соберём сообщение целиком (EndOfMessage).
            while (true)
            {
                // capacity = докуда вообще можно писать. Длина массива, но не выше потолка сообщения:
                // пул может вернуть массив больше запрошенного, а за лимит писать нельзя.
                var capacity = Math.Min(buffer.Length, maxMessageBytes);

                // Буфер заполнен под завязку (total == capacity), а сообщение ещё не кончилось: нужно больше места.
                if (total == capacity)
                {
                    // Расти некуда: массив уже размером с потолок. Отказываемся явно, а не принимаем
                    // сколь угодно большое сообщение. Это и есть защита от memory-DoS.
                    if (buffer.Length >= maxMessageBytes)
                        throw new InvalidOperationException(
                            $"WS message exceeds MaxMessageBytes={maxMessageBytes}");

                    // Удваиваем размер, но не выше потолка. (long)-каст, чтобы умножение не переполнило int.
                    var newSize = (int)Math.Min((long)buffer.Length * 2, maxMessageBytes);

                    // Берём из пула массив побольше, переносим в него уже накопленные total байт,
                    // старый возвращаем в пул, дальше работаем с новым.
                    var bigger = ArrayPool<byte>.Shared.Rent(newSize);
                    Array.Copy(buffer, bigger, total);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = bigger;

                    // Буфер сменился, пересчитываем доступную ёмкость.
                    capacity = Math.Min(buffer.Length, maxMessageBytes);
                }

                // Читаем следующий кадр в СВОБОДНУЮ часть буфера: срез начиная с total, длиной в остаток
                // места. Так кадры приклеиваются друг к другу, ничего не затирая.
                var result = await socket.ReceiveAsync(buffer.AsMemory(total, capacity - total), ct);

                // Пир закрыл соединение: полезного сообщения нет. Возвращаем буфер в пул и сообщаем, что закрыто.
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    return ReceivedMessage.Closed;
                }

                // Ноль байт и кадр не финальный: пир ничего не прислал и не закрылся. Без этой проверки
                // цикл крутился бы вхолостую на полном CPU. Падаем явно.
                if (result.Count == 0 && !result.EndOfMessage)
                    throw new InvalidOperationException("WS peer returned an empty non-final frame");

                // Сдвигаем счётчик на число реально полученных байт.
                total += result.Count;

                // Это был последний кадр: отдаём собранный буфер и его длину. Владение буфером переходит
                // в ReceivedMessage, он вернёт массив в пул на Dispose.
                if (result.EndOfMessage)
                    return ReceivedMessage.Message(buffer, total);

                // Иначе сообщение ещё не целое: идём за следующим кадром.
            }
        }
        catch
        {
            // Любой сбой на полпути (рост за потолок, обрыв, отмена): не теряем арендованный массив,
            // возвращаем его в пул и пробрасываем исключение дальше. Выше по стеку это ведёт к reconnect.
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }
}

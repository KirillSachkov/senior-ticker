using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Парсит один формат биржи (UTF-8 кадр) в нормализованный Tick. Одна реализация на формат
/// (расширяемость: новая биржа = новый парсер + коннектор). Допущение: один тик на кадр.
/// </summary>
public interface IMessageParser
{
    /// <returns>true + tick при успешном разборе; false при невалидном/нераспознанном кадре.</returns>
    bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick);
}

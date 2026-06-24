using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Проверка значений недоверенного фида. decimal по типу исключает NaN и Infinity; дальше проверяем
/// диапазон цены и объёма (нижняя и верхняя границы), окно времени (защищает окно дедупликации и
/// индекс по времени от абсурдных меток) и символ.
/// </summary>
public static class TickValidator
{
    public static readonly TimeSpan MaxPastSkew = TimeSpan.FromDays(7);
    public static readonly TimeSpan MaxFutureSkew = TimeSpan.FromMinutes(1);

    /// <summary>Верхняя граница цены и объёма по модулю. Колонка в базе это numeric(38,18), целая часть
    /// меньше 10^20. Значение с 21+ цифрой в целой части прошло бы парсер и нижнюю проверку, но уронило
    /// бы COPY (numeric field overflow), а это сбой конвейера и падение хоста одним кадром (удалённый
    /// DoS). 10^18 на порядки выше любой реальной цены или объёма и на два порядка ниже предела колонки.</summary>
    public const decimal MaxValue = 1_000_000_000_000_000_000m; // 10^18

    /// <summary>Верхняя граница длины тикера. Защищает память окна дедупликации и стоимость FNV-шардинга:
    /// без неё враждебный фид мог бы прислать символ до MaxMessageBytes, который попадёт в TickKey
    /// и осядет ключом в HashSet окна дедупликации на всё окно.</summary>
    public const int MaxSymbolLength = 32;

    public static bool IsValid(in Tick tick, TimeProvider time)
    {
        if (tick.Price <= 0m || tick.Price > MaxValue) return false;
        if (tick.Volume < 0m || tick.Volume > MaxValue) return false;
        if (!IsValidSymbol(tick.Symbol)) return false;

        var now = time.GetUtcNow();
        if (tick.ExchangeTimestamp < now - MaxPastSkew) return false;
        if (tick.ExchangeTimestamp > now + MaxFutureSkew) return false;
        return true;
    }

    // Белый список: латиница, цифры и разделители реальных тикеров (BTCUSDT, BTC/USD, XBT-USD, BTC_USD, BTC.D).
    // Отвергает мусор и гигантские метки до того, как они отравят окно дедупликации и индекс по времени.
    private static bool IsValidSymbol(string symbol)
    {
        if (string.IsNullOrEmpty(symbol) || symbol.Length > MaxSymbolLength) return false;
        foreach (var c in symbol)
        {
            var ok = c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9')
                or '/' or '-' or '_' or '.';
            if (!ok) return false;
        }
        return true;
    }
}

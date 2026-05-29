using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>
/// Гейт значений недоверенного фида (§11). decimal исключает NaN/Infinity по типу; проверяем
/// диапазон цены/объёма, окно времени (защищает окно дедупа и тайм-партиции от абсурдных меток) и символ.
/// </summary>
public static class TickValidator
{
    public static readonly TimeSpan MaxPastSkew = TimeSpan.FromDays(7);
    public static readonly TimeSpan MaxFutureSkew = TimeSpan.FromMinutes(1);

    /// <summary>Верхняя граница длины тикера. Защищает память окна дедупа и стоимость FNV-шардинга:
    /// без неё враждебный фид мог бы прислать символ до MaxMessageBytes, который попадёт в TickKey
    /// и осядет ключом в HashSet окна дедупа на всё окно.</summary>
    public const int MaxSymbolLength = 32;

    public static bool IsValid(in Tick tick, TimeProvider time)
    {
        if (tick.Price <= 0m) return false;
        if (tick.Volume < 0m) return false;
        if (!IsValidSymbol(tick.Symbol)) return false;

        var now = time.GetUtcNow();
        if (tick.ExchangeTimestamp < now - MaxPastSkew) return false;
        if (tick.ExchangeTimestamp > now + MaxFutureSkew) return false;
        return true;
    }

    // Whitelist: латиница/цифры + разделители реальных тикеров (BTCUSDT, BTC/USD, XBT-USD, BTC_USD, BTC.D).
    // Отвергает мусор/мохибейк/гигантские метки до того, как они отравят окно дедупа и тайм-партиции.
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

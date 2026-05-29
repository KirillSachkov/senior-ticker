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

    public static bool IsValid(in Tick tick, TimeProvider time)
    {
        if (tick.Price <= 0m) return false;
        if (tick.Volume < 0m) return false;
        if (string.IsNullOrEmpty(tick.Symbol)) return false;

        var now = time.GetUtcNow();
        if (tick.ExchangeTimestamp < now - MaxPastSkew) return false;
        if (tick.ExchangeTimestamp > now + MaxFutureSkew) return false;
        return true;
    }
}

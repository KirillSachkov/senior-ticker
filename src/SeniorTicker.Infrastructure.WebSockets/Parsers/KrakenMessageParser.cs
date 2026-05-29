using System.Globalization;
using System.Text.Json;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>
/// Парсер Kraken-подобного формата-массива: [channelId, [price, volume, time_sec], "trade", pair, tradeId].
/// Ручной Utf8JsonReader — формат позиционный, на DTO не ложится.
/// </summary>
public sealed class KrakenMessageParser : IMessageParser
{
    // Seconds bounds derived from shared UnixTime ms constants (static readonly — decimal division not const-foldable)
    private static readonly decimal MaxTimeSec = UnixTime.MaxMs / 1000m;
    private static readonly decimal MinTimeSec = UnixTime.MinMs / 1000m;

    public bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick)
    {
        tick = default;
        try
        {
            var reader = new Utf8JsonReader(utf8Frame);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray) return false;

            // [0] channelId — skip
            if (!reader.Read() || reader.TokenType != JsonTokenType.Number) return false;

            // [1] nested array [price, volume, time]
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray) return false;
            if (!ReadDecimalString(ref reader, out var price)) return false;
            if (!ReadDecimalString(ref reader, out var volume)) return false;
            if (!ReadDecimalString(ref reader, out var timeSec)) return false;
            if (!reader.Read() || reader.TokenType != JsonTokenType.EndArray) return false;

            // [2] "trade"
            if (!reader.Read() || reader.TokenType != JsonTokenType.String) return false;

            // [3] pair "BTC/USD"
            if (!reader.Read() || reader.TokenType != JsonTokenType.String) return false;
            var symbol = reader.GetString();
            if (string.IsNullOrEmpty(symbol)) return false;

            // [4] tradeId
            if (!reader.Read() || reader.TokenType != JsonTokenType.Number || !reader.TryGetInt64(out var tradeId))
                return false;

            // кадр должен заканчиваться здесь — отвергаем лишние хвостовые элементы
            if (!reader.Read() || reader.TokenType != JsonTokenType.EndArray) return false;

            // Guard against OverflowException on decimal→long cast and out-of-range ms values.
            // timeSec can be huge (e.g. decimal.MaxValue), so check before multiplying.
            // MaxTimeSec ≈ 2.53e11, well within decimal range, so we compare in seconds first.
            if (timeSec < MinTimeSec || timeSec > MaxTimeSec) return false;

            var ms = (long)(timeSec * 1000m);
            // Double-check after rounding (timeSec*1000 might still land outside bounds at the edges)
            if (ms < UnixTime.MinMs || ms > UnixTime.MaxMs) return false;

            tick = new Tick(Exchange.Kraken, symbol, price, volume,
                DateTimeOffset.FromUnixTimeMilliseconds(ms), tradeId, ingestTimestamp);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static bool ReadDecimalString(ref Utf8JsonReader reader, out decimal value)
    {
        value = 0m;
        if (!reader.Read() || reader.TokenType != JsonTokenType.String) return false;
        var s = reader.GetString();
        return decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }
}

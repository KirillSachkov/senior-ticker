using System.Globalization;
using System.Text.Json;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>Парсер Binance aggTrade-подобного JSON (source-gen STJ). Любой сбой → false (skip+count выше).</summary>
public sealed class BinanceMessageParser : IMessageParser
{
    public bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick)
    {
        tick = default;
        BinanceTradeDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize(utf8Frame, ExchangeMessageJsonContext.Default.BinanceTradeDto);
        }
        catch (JsonException)
        {
            return false;
        }

        if (dto is null || string.IsNullOrEmpty(dto.Symbol)
            || !decimal.TryParse(dto.Price, NumberStyles.Number, CultureInfo.InvariantCulture, out var price)
            || !decimal.TryParse(dto.Quantity, NumberStyles.Number, CultureInfo.InvariantCulture, out var volume))
        {
            return false;
        }

        tick = new Tick(
            Exchange.Binance, dto.Symbol, price, volume,
            DateTimeOffset.FromUnixTimeMilliseconds(dto.EventTimeMs),
            dto.AggTradeId, ingestTimestamp);
        return true;
    }
}

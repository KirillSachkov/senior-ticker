using System.Text.Json.Serialization;

namespace SeniorTicker.Infrastructure.WebSockets;

/// <summary>DTO формата Binance aggTrade-подобного: s=symbol, p=price, q=qty, T=trade time ms, a=agg id.</summary>
public sealed class BinanceTradeDto
{
    [JsonPropertyName("s")] public string? Symbol { get; set; }
    [JsonPropertyName("p")] public string? Price { get; set; }
    [JsonPropertyName("q")] public string? Quantity { get; set; }
    [JsonPropertyName("T")] public long EventTimeMs { get; set; }
    [JsonPropertyName("a")] public long AggTradeId { get; set; }
}

[JsonSourceGenerationOptions(
    PropertyNameCaseInsensitive = false,
    AllowOutOfOrderMetadataProperties = false,
    AllowDuplicateProperties = false)]
[JsonSerializable(typeof(BinanceTradeDto))]
public partial class ExchangeMessageJsonContext : JsonSerializerContext;

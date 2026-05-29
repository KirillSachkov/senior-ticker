using System.Globalization;

namespace SeniorTicker.MockExchange;

/// <summary>Генерирует тики и сериализует в формат конкретной биржи. tradeId монотонный.</summary>
public static class MockTickGenerator
{
    public static string Binance(string symbol, long tradeId, long nowMs)
    {
        var price = (50000 + tradeId % 100).ToString(CultureInfo.InvariantCulture);
        return $$"""{"s":"{{symbol}}","p":"{{price}}.50","q":"0.25","T":{{nowMs}},"a":{{tradeId}}}""";
    }

    public static string Kraken(string pair, long tradeId, long nowMs)
    {
        var price = (3000 + tradeId % 100).ToString(CultureInfo.InvariantCulture);
        var timeSec = (nowMs / 1000.0).ToString("F3", CultureInfo.InvariantCulture);
        return $$"""[42,["{{price}}.5","0.10","{{timeSec}}"],"trade","{{pair}}",{{tradeId}}]""";
    }

    public static string Csv(string symbol, long tradeId, long nowMs)
    {
        var price = (100 + tradeId % 50).ToString(CultureInfo.InvariantCulture);
        return $"{symbol},{price}.25,1.5,{nowMs},{tradeId}";
    }
}

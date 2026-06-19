namespace SeniorTicker.Host.Configuration;

/// <summary>Формат сообщений биржи → выбор парсера в <see cref="Ingestion.ConnectorFactory"/>.</summary>
public enum ExchangeFormat
{
    Binance = 0,
    Kraken = 1,
    Csv = 2,
}

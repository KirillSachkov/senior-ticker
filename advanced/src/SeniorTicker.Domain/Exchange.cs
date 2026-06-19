namespace SeniorTicker.Domain;

/// <summary>Источник котировок. byte — компактно для value-type Tick.</summary>
public enum Exchange : byte
{
    Unknown = 0,
    Binance = 1,
    Kraken = 2,
    Coinbase = 3,
    Mock = 4,
}

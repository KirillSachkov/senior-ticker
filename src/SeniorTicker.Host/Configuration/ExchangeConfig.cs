namespace SeniorTicker.Host.Configuration;

/// <summary>
/// Один источник в конфиге (<c>Exchanges[]</c>). Регистрируется только при <see cref="Enabled"/>
/// (#9: debug-коннектор не попадает в прод). <c>Symbols[]</c> намеренно отсутствует — mock стримит
/// фиксированный символ, per-symbol-подписка = мультиплексирование (growth path §10), а не dead-config.
/// </summary>
public sealed class ExchangeConfig
{
    public string Name { get; init; } = "";
    public ExchangeFormat Format { get; init; }
    public string Url { get; init; } = "";
    public bool Enabled { get; init; } = true;
}

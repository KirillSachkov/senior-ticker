namespace SeniorTicker.Host.Configuration;

/// <summary>
/// Корневой узел конфигурации приложения (биндится от корня): источники + ручки конвейера,
/// бюджет остановки, период метрик. Валидируется на старте (<see cref="SeniorTickerOptionsValidator"/>).
/// </summary>
public sealed class SeniorTickerOptions
{
    public List<ExchangeConfig> Exchanges { get; init; } = [];
    public PipelineConfig Pipeline { get; init; } = new();
    public ShutdownConfig Shutdown { get; init; } = new();
    public MetricsConfig Metrics { get; init; } = new();
}

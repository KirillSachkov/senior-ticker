namespace SeniorTicker.Host.Configuration;

/// <summary>
/// Бюджет двухфазной остановки (§5.4). <see cref="DrainTimeoutSeconds"/> ограничивает время дописи
/// буфера на остановке; <c>HostOptions.ShutdownTimeout</c> ставится строго больше (см. HostingExtensions).
/// </summary>
public sealed class ShutdownConfig
{
    public int DrainTimeoutSeconds { get; init; } = 30;
}

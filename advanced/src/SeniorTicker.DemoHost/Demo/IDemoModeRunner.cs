namespace SeniorTicker.DemoHost.Demo;

/// <summary>
/// Один режим сравнительного демо. Общий контракт для боевого <see cref="DemoModeRunner"/>
/// (Channels + шарды) и учебного <see cref="NaiveDemoModeRunner"/>.
/// </summary>
public interface IDemoModeRunner
{
    bool Running { get; }
    void ResetStats(DemoConfig config);
    Task StartAsync(DemoConfig config);
    Task StopAsync();
    ModeSnapshot Snapshot();
}

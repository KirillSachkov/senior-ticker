namespace SeniorTicker.Application;

/// <summary>Счётчики конвейера. Реализация — потокобезопасная (Interlocked), т.к. дёргается из многих стадий.</summary>
public interface IMetricsSink
{
    void OnReceived(long n = 1);
    void OnDeduplicated(long n = 1);
    void OnWritten(long n);
    void OnDropped(long n = 1);
}

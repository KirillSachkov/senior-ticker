using System.Diagnostics.Metrics;
using SeniorTicker.Application;

namespace SeniorTicker.Host.Observability;

/// <summary>
/// Порт метрик через стандартные инструменты OpenTelemetry (<see cref="System.Diagnostics.Metrics"/>):
/// каждое событие конвейера — инкремент монотонного <see cref="Counter{T}"/> на Meter "SeniorTicker".
/// MeterProvider (DI) периодически экспортирует их в стандартном формате (console); любой OTLP/Prometheus
/// экспортёр подключается тем же AddMeter без правок конвейера.
/// <para>
/// Раньше счётчики и снимок были самописными (#7: баг ученика с частичным reset рассинхронизировал
/// метрики). Стандартный Counter аддитивен и монотонен по конструкции — снимать и сбрасывать вручную
/// нечего, reset-рассинхрона тут больше нет. gap = received − written и rate считает потребитель.
/// </para>
/// </summary>
public sealed class MetricsSink : IMetricsSink, IDisposable
{
    public const string MeterName = "SeniorTicker";

    private readonly Meter _meter;
    private readonly Counter<long> _received;
    private readonly Counter<long> _deduplicated;
    private readonly Counter<long> _written;
    private readonly Counter<long> _dropped;

    public MetricsSink()
    {
        _meter = new Meter(MeterName);
        _received = _meter.CreateCounter<long>("ticker.received", "{tick}", "Тики, принятые из коннекторов.");
        _deduplicated = _meter.CreateCounter<long>("ticker.deduplicated", "{tick}", "Повторы, отброшенные дедупликатором.");
        _written = _meter.CreateCounter<long>("ticker.written", "{tick}", "Тики, записанные в PostgreSQL.");
        _dropped = _meter.CreateCounter<long>("ticker.dropped", "{tick}", "Тики, отброшенные на приёме (формат или валидация).");
    }

    /// <summary>Meter со всеми инструментами сервиса — точка для gauge'ов глубины каналов и для тестов.</summary>
    public Meter Meter => _meter;

    public void OnReceived(long n = 1) => _received.Add(n);
    public void OnDeduplicated(long n = 1) => _deduplicated.Add(n);
    public void OnWritten(long n) => _written.Add(n);
    public void OnDropped(long n = 1) => _dropped.Add(n);

    public void Dispose() => _meter.Dispose();
}
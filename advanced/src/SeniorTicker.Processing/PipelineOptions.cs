namespace SeniorTicker.Processing;

/// <summary>Настройки конвейера. Дефолты рассчитаны на нагрузку теста (100 в секунду). P и K выставлены в минимум.</summary>
public sealed class PipelineOptions
{
    public int ShardCount { get; init; } = 1;            // P: число линий дедупликации (single-writer)
    public int WriterCount { get; init; } = 2;           // K: число writer-воркеров
    public int IngestCapacity { get; init; } = 1000;     // ограниченный вход: при заполнении продюсеры ждут
    public int ShardCapacity { get; init; } = 1000;      // ограниченный канал на шард
    public int BatchChannelCapacity { get; init; } = 8;  // ограниченный канал батчей: при заполнении шарды ждут
    public int BatchMaxSize { get; init; } = 900;        // N: сброс по размеру. 900×88 байт = 79.2 КБ, меньше порога LOH 85 КБ
    public TimeSpan BatchMaxDelay { get; init; } = TimeSpan.FromMilliseconds(100); // T: сброс по времени
    public TimeSpan DedupWindow { get; init; } = TimeSpan.FromMinutes(1);
}

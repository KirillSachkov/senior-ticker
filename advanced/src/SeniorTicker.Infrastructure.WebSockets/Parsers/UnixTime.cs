namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>Границы валидного диапазона DateTimeOffset.FromUnixTimeMilliseconds (мс).</summary>
internal static class UnixTime
{
    public const long MinMs = -62135596800000L; // DateTimeOffset.MinValue
    public const long MaxMs = 253402300799999L; // DateTimeOffset.MaxValue
}

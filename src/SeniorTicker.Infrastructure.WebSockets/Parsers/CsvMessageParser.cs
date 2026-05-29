using System.Buffers.Text;
using System.Globalization;
using System.Text;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>Парсер CSV-кадра: symbol,price,volume,epochMs,id. Демонстрирует не-JSON формат.</summary>
public sealed class CsvMessageParser : IMessageParser
{
    // Valid range for DateTimeOffset.FromUnixTimeMilliseconds
    private const long MinUnixMs = -62135596800000L;
    private const long MaxUnixMs = 253402300799999L;

    public bool TryParse(ReadOnlySpan<byte> utf8Frame, DateTimeOffset ingestTimestamp, out Tick tick)
    {
        tick = default;
        Span<Range> fields = stackalloc Range[6];
        var text = utf8Frame;
        var count = Split(text, (byte)',', fields);
        if (count != 5) return false;

        var symbolBytes = text[fields[0]];
        if (symbolBytes.IsEmpty) return false;
        var symbol = Encoding.UTF8.GetString(symbolBytes);

        if (!TryParseDecimal(text[fields[1]], out var price)) return false;
        if (!TryParseDecimal(text[fields[2]], out var volume)) return false;
        if (!Utf8Parser.TryParse(text[fields[3]], out long epochMs, out _)) return false;
        if (!Utf8Parser.TryParse(text[fields[4]], out long id, out _)) return false;

        // Guard: epochMs must be in valid DateTimeOffset range before calling FromUnixTimeMilliseconds
        if (epochMs < MinUnixMs || epochMs > MaxUnixMs) return false;

        tick = new Tick(Exchange.Coinbase, symbol, price, volume,
            DateTimeOffset.FromUnixTimeMilliseconds(epochMs), id, ingestTimestamp);
        return true;
    }

    private static int Split(ReadOnlySpan<byte> s, byte sep, Span<Range> ranges)
    {
        var n = 0; var start = 0;
        for (var i = 0; i < s.Length && n < ranges.Length; i++)
        {
            if (s[i] == sep) { ranges[n++] = new Range(start, i); start = i + 1; }
        }
        if (n < ranges.Length) ranges[n++] = new Range(start, s.Length);
        return n;
    }

    private static bool TryParseDecimal(ReadOnlySpan<byte> utf8, out decimal value)
        => decimal.TryParse(utf8, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
}

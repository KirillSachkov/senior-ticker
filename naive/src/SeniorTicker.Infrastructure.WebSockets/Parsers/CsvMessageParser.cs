using System.Buffers.Text;
using System.Globalization;
using System.Text;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Infrastructure.WebSockets.Parsers;

/// <summary>Парсер CSV-кадра: symbol,price,volume,epochMs,id. Демонстрирует не-JSON формат.</summary>
public sealed class CsvMessageParser : IMessageParser
{
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
        // Utf8Parser парсит ПРЕФИКС: без проверки consumed == длины поля "1700000000000XYZ" приняло бы 1700000000000.
        // Требуем полного потребления — симметрично строгости decimal-полей и Kraken (отвергает хвостовые элементы).
        if (!TryParseFullInt64(text[fields[3]], out long epochMs)) return false;
        if (!TryParseFullInt64(text[fields[4]], out long id)) return false;

        // Guard: epochMs must be in valid DateTimeOffset range before calling FromUnixTimeMilliseconds
        if (epochMs < UnixTime.MinMs || epochMs > UnixTime.MaxMs) return false;

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

    private static bool TryParseFullInt64(ReadOnlySpan<byte> utf8, out long value)
        => Utf8Parser.TryParse(utf8, out value, out var consumed) && consumed == utf8.Length;
}

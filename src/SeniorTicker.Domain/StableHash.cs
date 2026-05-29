using System.Text;

namespace SeniorTicker.Domain;

/// <summary>
/// Детерминированный хеш для маршрутизации символ→шард. НЕ String.GetHashCode:
/// он рандомизирован per-process (с .NET Core) → один символ маппится в РАЗНЫЕ шарды
/// на разных инстансах/рестартах, что ломает инвариант шардинга. FNV-1a 64-bit по
/// UTF-8 байтам. Результат — ulong (неотрицательный по построению), поэтому % не даёт
/// отрицательного индекса (профилактика класса проблемы №3 из код-ревью).
/// </summary>
public static class StableHash
{
    public static ulong Fnv1a64(ReadOnlySpan<char> value)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        Span<byte> buffer = stackalloc byte[256];
        var maxBytes = Encoding.UTF8.GetByteCount(value);
        byte[]? rented = null;
        var bytes = maxBytes <= buffer.Length
            ? buffer[..Encoding.UTF8.GetBytes(value, buffer)]
            : RentAndEncode(value, maxBytes, out rented);

        var hash = offset;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }

        if (rented is not null)
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        return hash;

        static ReadOnlySpan<byte> RentAndEncode(ReadOnlySpan<char> v, int max, out byte[] rented)
        {
            rented = System.Buffers.ArrayPool<byte>.Shared.Rent(max);
            var written = Encoding.UTF8.GetBytes(v, rented);
            return rented.AsSpan(0, written);
        }
    }

    /// <summary>Индекс шарда [0, shardCount). Неотрицательный по построению.</summary>
    public static int ShardOf(string symbol, int shardCount)
    {
        ArgumentException.ThrowIfNullOrEmpty(symbol);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        return (int)(Fnv1a64(symbol) % (ulong)shardCount);
    }
}

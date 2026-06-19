using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class StableHashTests
{
    [Fact]
    public void Fnv1a64_is_deterministic_across_calls()
    {
        Assert.Equal(StableHash.Fnv1a64("BTCUSDT"), StableHash.Fnv1a64("BTCUSDT"));
        Assert.NotEqual(StableHash.Fnv1a64("BTCUSDT"), StableHash.Fnv1a64("ETHUSDT"));
    }

    [Fact]
    public void Fnv1a64_matches_golden_vectors()
    {
        // ARCH-1: пиннит именно FNV-1a. Подмена тела на String.GetHashCode (рандомизирован per-process →
        // ломает кросс-инстансный/кросс-рестартный шардинг, проблема №2/№3) оставила бы тест
        // детерминизма зелёным, но эти golden-вектора — нет. Значения вычислены независимо.
        Assert.Equal(14695981039346656037UL, StableHash.Fnv1a64(""));        // FNV offset basis (пустой вход)
        Assert.Equal(4495733446125262380UL, StableHash.Fnv1a64("BTCUSDT"));
        Assert.Equal(9177286476152214768UL, StableHash.Fnv1a64("ETHUSDT"));
    }

    [Theory]
    [InlineData("BTCUSDT", 4)]
    [InlineData("ETHUSDT", 1)]
    [InlineData("a", 8)]
    public void ShardOf_is_in_range_and_stable(string symbol, int shards)
    {
        var s1 = StableHash.ShardOf(symbol, shards);
        var s2 = StableHash.ShardOf(symbol, shards);
        Assert.InRange(s1, 0, shards - 1);
        Assert.Equal(s1, s2);
    }

    [Fact]
    public void ShardOf_rejects_invalid_input()
    {
        Assert.Throws<ArgumentException>(() => StableHash.ShardOf("", 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => StableHash.ShardOf("BTC", 0));
    }
}

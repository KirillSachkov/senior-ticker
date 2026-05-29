using Microsoft.Extensions.Time.Testing;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

public class SlidingWindowDeduplicatorTests
{
    private static SlidingWindowDeduplicator Make(FakeTimeProvider time, TimeSpan? window = null)
        => new(window ?? TimeSpan.FromMinutes(1), time);

    [Fact]
    public void First_occurrence_is_not_duplicate_second_is()
    {
        var time = new FakeTimeProvider();
        var dedup = Make(time);
        var t = TickFactory.New("BTCUSDT", sourceId: 1);

        Assert.False(dedup.IsDuplicate(t));   // первый раз — новый
        Assert.True(dedup.IsDuplicate(t));    // повтор — дубликат
    }

    [Fact]
    public void Distinct_ticks_same_symbol_same_timestamp_are_NOT_collapsed()
    {
        // КЛАСС ПРОБЛЕМЫ №2: точный ключ с тайбрейкером SourceId — два разных тика
        // в одну миллисекунду должны остаться двумя разными, не "дубликатом".
        var time = new FakeTimeProvider();
        var dedup = Make(time);
        var ts = DateTimeOffset.UnixEpoch;

        Assert.False(dedup.IsDuplicate(TickFactory.New("BTCUSDT", sourceId: 1, exchangeTs: ts)));
        Assert.False(dedup.IsDuplicate(TickFactory.New("BTCUSDT", sourceId: 2, exchangeTs: ts)));
    }

    [Fact]
    public void Key_outside_time_window_is_treated_as_new_again()
    {
        var time = new FakeTimeProvider();
        var dedup = Make(time, window: TimeSpan.FromSeconds(10));
        var t = TickFactory.New("BTCUSDT", sourceId: 1);

        Assert.False(dedup.IsDuplicate(t));
        time.Advance(TimeSpan.FromSeconds(11));  // окно истекло → запись вытеснена
        Assert.False(dedup.IsDuplicate(t));      // снова новый
    }

    [Fact]
    public void Duplicate_within_window_stays_duplicate()
    {
        var time = new FakeTimeProvider();
        var dedup = Make(time, window: TimeSpan.FromSeconds(10));
        var t = TickFactory.New("BTCUSDT", sourceId: 1);

        Assert.False(dedup.IsDuplicate(t));
        time.Advance(TimeSpan.FromSeconds(5));   // ещё в окне
        Assert.True(dedup.IsDuplicate(t));
    }
}

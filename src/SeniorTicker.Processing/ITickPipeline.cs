using System.Threading.Channels;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing;

/// <summary>
/// Внешний контракт конвейера, который видит Host: куда писать тики (<see cref="Input"/>), как
/// запустить прокачку (<see cref="RunAsync"/>) и глубины каналов для метрик. Боевая реализация —
/// <see cref="TickPipeline"/> (Channels, шарды, батчи, двухфазный дренаж); учебный антипример,
/// включаемый тумблером <c>Pipeline:Mode=Naive</c>, — <c>Naive.NaivePipeline</c>.
/// </summary>
public interface ITickPipeline
{
    /// <summary>Точка входа для продюсеров (коннекторов).</summary>
    ChannelWriter<Tick> Input { get; }

    int IngestDepth { get; }
    int BatchDepth { get; }
    int[] ShardDepths { get; }

    Task RunAsync(CancellationToken ct);
}

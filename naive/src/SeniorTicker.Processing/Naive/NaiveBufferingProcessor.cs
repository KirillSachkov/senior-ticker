using System.Threading.Channels;
using SeniorTicker.Application;
using SeniorTicker.Domain;

namespace SeniorTicker.Processing.Naive;

/// <summary>
/// «Простой вариант» остановки: один общий токен останавливает и приём, и запись. Принятые,
/// но ещё не записанные тики теряются при штатной остановке. Антипример к двухфазному дренажу
/// (Input.Complete() → дочитать очереди → дописать) в <see cref="TickPipeline"/>.
/// </summary>
public sealed class NaiveBufferingProcessor(ITickSink sink)
{
    private readonly Channel<Tick> _input = Channel.CreateUnbounded<Tick>();

    public ChannelWriter<Tick> Input => _input.Writer;

    public async Task RunAsync(CancellationToken hostToken)
    {
        var buffer = new List<Tick>();
        try
        {
            await foreach (var tick in _input.Reader.ReadAllAsync(hostToken).ConfigureAwait(false))
                buffer.Add(tick);
        }
        catch (OperationCanceledException)
        {
            // hostToken отменён → цикл чтения оборван, buffer НЕ дописан = потеря данных.
            return;
        }

        // сюда попадаем только при штатном завершении канала (которого при отмене не будет)
        if (buffer.Count > 0)
            await sink.WriteBatchAsync(buffer.ToArray(), CancellationToken.None).ConfigureAwait(false);
    }
}

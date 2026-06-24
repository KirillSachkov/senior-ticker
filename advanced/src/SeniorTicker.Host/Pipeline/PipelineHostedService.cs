using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Pipeline;

/// <summary>
/// Владелец жизненного цикла конвейера. StartAsync запускает <see cref="TickPipeline.RunAsync"/> на
/// собственном токене отмены, а не на токене остановки хоста. Штатная остановка идёт через
/// <c>Input.Complete()</c>, а не через отмену: отмена означала бы потерю буфера.
///
/// StopAsync это вторая фаза остановки. Коннекторы к этому моменту уже погашены (они зарегистрированы
/// позже, поэтому останавливаются раньше). Закрываем вход и ждём, пока конвейер дочитает остаток, в
/// пределах <see cref="ShutdownConfig.DrainTimeoutSeconds"/>. Если не успел, принудительно отменяем и
/// пишем явную ошибку в лог.
///
/// Если конвейер падает на ходу (например, отказал sink), это фатальный сбой: пишем Critical и
/// останавливаем приложение. В StopAsync такой сбой пробрасывается ровно один раз, чтобы код выхода
/// был ненулевым (см. ветки ниже).
/// </summary>
public sealed class PipelineHostedService(
    TickPipeline pipeline,
    ShutdownConfig shutdown,
    IHostApplicationLifetime lifetime,
    TimeProvider time,
    ILogger<PipelineHostedService> logger) : IHostedService, IDisposable
{
    private readonly CancellationTokenSource _abort = new();
    private Task? _run;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _run = pipeline.RunAsync(_abort.Token);

        // fail-fast: если конвейер падает на ходу (отказал sink), не оставляем коннекторы лить в
        // заблокированный канал. Отдельный наблюдатель ждёт _run и при ошибке останавливает приложение.
        _ = ObservePipelineFaultAsync(_run);

        logger.LogInformation("Pipeline started.");
        return Task.CompletedTask;
    }

    // Ждёт завершения конвейера в фоне. Отмена (штатная или аварийная) это не сбой. Любое другое
    // исключение это фатальный сбой (например, отказал sink): пишем Critical и останавливаем хост.
    private async Task ObservePipelineFaultAsync(Task run)
    {
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            // штатная остановка или принудительная отмена: приложение не трогаем
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Pipeline faulted, stopping application.");
            lifetime.StopApplication();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_run is null)
            return;

        // Закрываем вход: тиков больше не будет. Это запускает дочитывание остатка по конвейеру
        // (router, шарды, батчи, writers).
        pipeline.Input.Complete();

        var drain = TimeSpan.FromSeconds(shutdown.DrainTimeoutSeconds);
        try
        {
            // Ждём дочитывания остатка в пределах дедлайна; время берём из TimeProvider (детерминизм
            // в тестах). Токен принудительной отмены тут не трогаем: это штатный путь, конвейер
            // завершается сам.
            await _run.WaitAsync(drain, time, cancellationToken);
            logger.LogInformation("Pipeline drained cleanly.");
            return;
        }
        catch (TimeoutException)
        {
            logger.LogError("Drain exceeded {Drain}s, forcing abort; buffered ticks may be lost.",
                shutdown.DrainTimeoutSeconds);
        }
        catch (OperationCanceledException)
        {
            // Дедлайн остановки хоста исчерпан раньше нашего, принудительно отменяем.
            logger.LogError("Shutdown deadline hit before drain completed, forcing abort.");
        }
        catch (Exception ex)
        {
            // Фатальный сбой конвейера (отказал sink) всплыл из WaitAsync: это не таймаут и не отмена.
            // Единственное место, где пробрасываем такой сбой на этом пути, чтобы код выхода был ненулевым.
            logger.LogError(ex, "Pipeline faulted on shutdown, surfacing for non-zero exit.");
            throw;
        }

        // Сюда попадаем только по таймауту или отмене: принудительно отменяем и смотрим результат.
        await ForceAbortAsync();

        // Если принудительная отмена вскрыла фатальный сбой, а не чистую отмену, пробрасываем его.
        if (_run.IsFaulted)
            await _run;
    }

    private async Task ForceAbortAsync()
    {
        await _abort.CancelAsync();
        try
        {
            await _run!;
        }
        catch (OperationCanceledException)
        {
            // Ожидаемо при принудительной отмене: чистая отмена.
        }
        catch (Exception)
        {
            // Фатальный сбой не глотаем: _run.IsFaulted останется true, и StopAsync пробросит его.
        }
    }

    public void Dispose() => _abort.Dispose();
}

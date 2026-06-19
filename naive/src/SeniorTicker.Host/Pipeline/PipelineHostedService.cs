using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Processing;

namespace SeniorTicker.Host.Pipeline;

/// <summary>
/// Владелец жизненного цикла конвейера. <c>StartAsync</c> запускает <see cref="TickPipeline.RunAsync"/>
/// на <b>внутреннем abort-токене</b> (НЕ host-stop): штатный дренаж управляется <c>Input.Complete()</c>,
/// а не отменой (отмена = abort, потеря буфера). <c>StopAsync</c> — фаза ② §5.4: коннекторы уже
/// погашены (зарегистрированы позже → остановлены раньше), завершаем вход и ждём естественный дренаж
/// в пределах <see cref="ShutdownConfig.DrainTimeoutSeconds"/>; превышение → форс-abort с громким логом.
/// Fail-fast (#C1): фолт конвейера (фатал sink'а) → <c>Critical</c> + StopApplication; в <c>StopAsync</c>
/// он ре-сёрфейсится РОВНО один раз для ненулевого exit (см. явные ветки ниже).
/// </summary>
public sealed class PipelineHostedService(
    ITickPipeline pipeline,
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

        // fail-fast: если конвейер падает на ходу (мёртвый sink), не оставляем коннекторы лить
        // в заблокированный канал — валим приложение корректным StopApplication (#C1).
        _ = _run.ContinueWith(
            t =>
            {
                logger.LogCritical(t.Exception, "Pipeline faulted — stopping application.");
                lifetime.StopApplication();
            },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        logger.LogInformation("Pipeline started.");
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_run is null)
            return;

        // ① «больше тиков не будет» → запускает естественный дренаж (router → шарды → батчи → writers)
        pipeline.Input.Complete();

        var drain = TimeSpan.FromSeconds(shutdown.DrainTimeoutSeconds);
        try
        {
            // ② ждём дренаж в пределах drain-дедлайна на инъектированном TimeProvider (детерминизм в тестах).
            // НЕ дёргаем abort — это штатный путь. Чистое завершение — единственное место «drained cleanly».
            await _run.WaitAsync(drain, time, cancellationToken);
            logger.LogInformation("Pipeline drained cleanly.");
            return;
        }
        catch (TimeoutException)
        {
            logger.LogError("Drain exceeded {Drain}s — forcing abort; buffered ticks may be lost.",
                shutdown.DrainTimeoutSeconds);
        }
        catch (OperationCanceledException)
        {
            // host ShutdownTimeout исчерпан раньше нашего drain — форсим
            logger.LogError("Shutdown deadline hit before drain completed — forcing abort.");
        }
        catch (Exception ex)
        {
            // Фатал конвейера (#C1: мёртвый sink) всплыл из WaitAsync — это НЕ таймаут и НЕ отмена.
            // Единственная точка ре-сёрфейса на fault-пути: поднимаем, чтобы exit был ненулевым.
            logger.LogError(ex, "Pipeline faulted on shutdown — surfacing for non-zero exit.");
            throw;
        }

        // Сюда попадаем только по Timeout/OCE: форсируем abort и наблюдаем результат.
        await ForceAbortAsync();

        // Если форс-abort вскрыл фатал (а не чистую отмену) — поднимаем его (вторая, abort-ветка ре-сёрфейса).
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
            // ожидаемо при форс-abort — чистая отмена
        }
        catch (Exception)
        {
            // фатал НЕ глотаем: _run.IsFaulted останется true → переподнимется в StopAsync
        }
    }

    public void Dispose() => _abort.Dispose();
}

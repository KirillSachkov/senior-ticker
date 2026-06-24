using Microsoft.Extensions.Options;
using SeniorTicker.Host.Security;

namespace SeniorTicker.Host.Configuration;

/// <summary>
/// Проверка на старте: каждый включённый источник обязан иметь имя и безопасный URL (wss, либо ws на
/// loopback). Выключенные источники не проверяются (их и не регистрируем). При
/// <c>ValidateOnStart()</c> провал даёт <see cref="OptionsValidationException"/>, и хост не стартует.
/// </summary>
public sealed class SeniorTickerOptionsValidator : IValidateOptions<SeniorTickerOptions>
{
    public ValidateOptionsResult Validate(string? name, SeniorTickerOptions options)
    {
        var failures = new List<string>();

        // Бюджет остановки: значение <= 0 уронило бы каждую штатную остановку в принудительный обрыв
        // (потеря буфера), а отрицательное дало бы ArgumentOutOfRange в WaitAsync. Проверяем на старте, как wss.
        if (options.Shutdown.DrainTimeoutSeconds <= 0)
            failures.Add("Shutdown:DrainTimeoutSeconds must be > 0.");

        var seenNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var ex in options.Exchanges)
        {
            if (!ex.Enabled)
                continue;

            if (string.IsNullOrWhiteSpace(ex.Name))
            {
                failures.Add("An enabled exchange has an empty Name.");
                continue;
            }

            if (!seenNames.Add(ex.Name))
                failures.Add($"Duplicate exchange name '{ex.Name}'.");

            var urlError = TransportSecurity.Validate(ex.Name, ex.Url);
            if (urlError is not null)
                failures.Add(urlError);
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}

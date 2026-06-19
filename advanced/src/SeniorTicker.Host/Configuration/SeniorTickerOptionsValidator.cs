using Microsoft.Extensions.Options;
using SeniorTicker.Host.Security;

namespace SeniorTicker.Host.Configuration;

/// <summary>
/// Гейт старта (#9 + кольцо 1): каждый <b>enabled</b> источник обязан иметь имя и безопасный URL
/// (wss, либо ws на loopback). Disabled-источники не проверяются (их и не регистрируем). С
/// <c>ValidateOnStart()</c> провал → <see cref="OptionsValidationException"/> и хост не стартует.
/// </summary>
public sealed class SeniorTickerOptionsValidator : IValidateOptions<SeniorTickerOptions>
{
    public ValidateOptionsResult Validate(string? name, SeniorTickerOptions options)
    {
        var failures = new List<string>();

        // Дренаж-бюджет (§5.4): <= 0 уронил бы каждый штатный shutdown в форс-abort (потеря буфера,
        // ровно риск #5), а отрицательный — ArgumentOutOfRange в WaitAsync. Гейтим у старта, как wss.
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

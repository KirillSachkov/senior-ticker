namespace SeniorTicker.Host.Security;

/// <summary>
/// Кольцо 1 (транспорт, §11) + killer проблемы #9 (debug-коннектор в проде): WebSocket-URL обязан
/// быть <c>wss://</c>; <c>ws://</c> допустим только на loopback (dev/mock). Любой другой вариант —
/// fail boot. Вызывается и validator'ом (ValidateOnStart), и фабрикой коннекторов (defense-in-depth).
/// </summary>
public static class TransportSecurity
{
    /// <returns>null — URL валиден; иначе человекочитаемая причина отказа.</returns>
    public static string? Validate(string name, string rawUrl)
    {
        if (!Uri.TryCreate(rawUrl, UriKind.Absolute, out var url))
            return $"Connector '{name}': URL '{rawUrl}' is not an absolute URI.";

        if (url.Scheme == Uri.UriSchemeWss)
            return null;
        if (url.Scheme == Uri.UriSchemeWs && url.IsLoopback)
            return null;

        return $"Connector '{name}': insecure URL '{rawUrl}'. Use wss://, or ws:// only on loopback (dev/mock).";
    }

    /// <summary>Бросает <see cref="InvalidOperationException"/> при невалидном URL (fail boot).</summary>
    public static Uri ValidateOrThrow(string name, string rawUrl)
    {
        var error = Validate(name, rawUrl);
        if (error is not null)
            throw new InvalidOperationException(error);
        return new Uri(rawUrl, UriKind.Absolute);
    }
}

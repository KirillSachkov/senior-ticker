namespace SeniorTicker.Application;

/// <summary>
/// Одна запущенная WS-биржа: connect → receive → parse → validate → ingest, с автопереподключением.
/// Работает до отмены ct. SRP: один коннектор на источник; падение одного не валит другие.
/// </summary>
public interface IExchangeConnector
{
    string Name { get; }
    Task RunAsync(CancellationToken ct);
}

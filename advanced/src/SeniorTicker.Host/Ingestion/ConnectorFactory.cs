using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using SeniorTicker.Application;
using SeniorTicker.Host.Configuration;
using SeniorTicker.Host.Security;
using SeniorTicker.Infrastructure.WebSockets;
using SeniorTicker.Infrastructure.WebSockets.Parsers;

namespace SeniorTicker.Host.Ingestion;

/// <summary>
/// Строит коннекторы из конфига: только включённые, парсер по <see cref="ExchangeFormat"/> (новая
/// биржа это новая ветка плюс класс парсера), плюс повторная проверка wss-URL (защита в несколько
/// слоёв). Socket-factory отдаёт <see cref="ClientWebSocket"/> с дефолтной проверкой сертификата:
/// никогда не подменяем callback на <c>return true</c>.
/// </summary>
public sealed class ConnectorFactory(
    ITickIngestor ingestor,
    IMetricsSink metrics,
    TimeProvider time,
    ILoggerFactory loggerFactory)
{
    public IReadOnlyList<IExchangeConnector> CreateEnabled(IEnumerable<ExchangeConfig> configs)
    {
        var connectors = new List<IExchangeConnector>();
        foreach (var cfg in configs)
        {
            if (!cfg.Enabled)
                continue;

            var url = TransportSecurity.ValidateOrThrow(cfg.Name, cfg.Url);
            var options = new WebSocketConnectorOptions
            {
                Name = cfg.Name,
                Url = url,
            };
            var logger = loggerFactory.CreateLogger($"Connector.{cfg.Name}");
            connectors.Add(new WebSocketConnectorBase(
                options,
                CreateParser(cfg.Format),
                ingestor,
                metrics,
                time,
                logger,
                static () => new ClientWebSocket()));
        }

        return connectors;
    }

    private static IMessageParser CreateParser(ExchangeFormat format) => format switch
    {
        ExchangeFormat.Binance => new BinanceMessageParser(),
        ExchangeFormat.Kraken => new KrakenMessageParser(),
        ExchangeFormat.Csv => new CsvMessageParser(),
        _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unknown exchange format."),
    };
}

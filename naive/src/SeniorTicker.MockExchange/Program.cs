using SeniorTicker.MockExchange;

var builder = WebApplication.CreateBuilder(args);
var app = MockExchangeApp.Build(builder);
app.Run();

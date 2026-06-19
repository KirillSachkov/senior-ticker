FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/SeniorTicker.Domain/SeniorTicker.Domain.csproj src/SeniorTicker.Domain/
COPY src/SeniorTicker.Domain/packages.lock.json src/SeniorTicker.Domain/
COPY src/SeniorTicker.Application/SeniorTicker.Application.csproj src/SeniorTicker.Application/
COPY src/SeniorTicker.Application/packages.lock.json src/SeniorTicker.Application/
COPY src/SeniorTicker.Processing/SeniorTicker.Processing.csproj src/SeniorTicker.Processing/
COPY src/SeniorTicker.Processing/packages.lock.json src/SeniorTicker.Processing/
COPY src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj src/SeniorTicker.DemoHost/
COPY src/SeniorTicker.DemoHost/packages.lock.json src/SeniorTicker.DemoHost/
RUN dotnet restore src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj

COPY src src
RUN dotnet publish src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj -c Release --no-restore -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "SeniorTicker.DemoHost.dll"]

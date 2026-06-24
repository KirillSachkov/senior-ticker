FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Общие MSBuild-props лежат в корне репозитория (наследуются проектами вверх по дереву каталогов).
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY advanced/src/SeniorTicker.Domain/SeniorTicker.Domain.csproj advanced/src/SeniorTicker.Domain/
COPY advanced/src/SeniorTicker.Domain/packages.lock.json advanced/src/SeniorTicker.Domain/
COPY advanced/src/SeniorTicker.Application/SeniorTicker.Application.csproj advanced/src/SeniorTicker.Application/
COPY advanced/src/SeniorTicker.Application/packages.lock.json advanced/src/SeniorTicker.Application/
COPY advanced/src/SeniorTicker.Processing/SeniorTicker.Processing.csproj advanced/src/SeniorTicker.Processing/
COPY advanced/src/SeniorTicker.Processing/packages.lock.json advanced/src/SeniorTicker.Processing/
COPY advanced/src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj advanced/src/SeniorTicker.DemoHost/
COPY advanced/src/SeniorTicker.DemoHost/packages.lock.json advanced/src/SeniorTicker.DemoHost/
RUN dotnet restore advanced/src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj

COPY advanced/src advanced/src
RUN dotnet publish advanced/src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj -c Release --no-restore -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "SeniorTicker.DemoHost.dll"]

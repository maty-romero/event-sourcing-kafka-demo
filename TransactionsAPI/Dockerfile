FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 5087

ENV ASPNETCORE_URLS=http://+:5087

USER app
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG configuration=Release
WORKDIR /src
COPY ["TransactionsAPI/TransactionsAPI.csproj", "TransactionsAPI/"]
RUN dotnet restore "TransactionsAPI/TransactionsAPI.csproj"
COPY . .
WORKDIR "/src/TransactionsAPI"
RUN dotnet build "TransactionsAPI.csproj" -c $configuration -o /app/build

FROM build AS publish
ARG configuration=Release
RUN dotnet publish "TransactionsAPI.csproj" -c $configuration -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TransactionsAPI.dll"]

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Bucktuality.Gateway/Bucktuality.Gateway.csproj Bucktuality.Gateway/
COPY Bucktuality.Shared/Bucktuality.Shared.csproj Bucktuality.Shared/

RUN dotnet restore Bucktuality.Gateway/Bucktuality.Gateway.csproj

COPY . .

RUN dotnet publish Bucktuality.Gateway/Bucktuality.Gateway.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 5000

ENV ASPNETCORE_URLS=http://+:5000

ENTRYPOINT ["dotnet", "Bucktuality.Gateway.dll"]
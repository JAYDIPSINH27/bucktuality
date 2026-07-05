FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Bucktuality.RealtimeService/Bucktuality.RealtimeService.csproj Bucktuality.RealtimeService/
COPY Bucktuality.Shared/Bucktuality.Shared.csproj Bucktuality.Shared/

RUN dotnet restore Bucktuality.RealtimeService/Bucktuality.RealtimeService.csproj

COPY . .

RUN dotnet publish Bucktuality.RealtimeService/Bucktuality.RealtimeService.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 5005

ENV ASPNETCORE_URLS=http://+:5005

ENTRYPOINT ["dotnet", "Bucktuality.RealtimeService.dll"]
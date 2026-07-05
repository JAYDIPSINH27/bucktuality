FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Bucktuality.ModerationService/Bucktuality.ModerationService.csproj Bucktuality.ModerationService/
COPY Bucktuality.Shared/Bucktuality.Shared.csproj Bucktuality.Shared/

RUN dotnet restore Bucktuality.ModerationService/Bucktuality.ModerationService.csproj

COPY . .

RUN dotnet publish Bucktuality.ModerationService/Bucktuality.ModerationService.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 5012

ENV ASPNETCORE_URLS=http://+:5012

ENTRYPOINT ["dotnet", "Bucktuality.ModerationService.dll"]
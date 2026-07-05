FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Bucktuality.SessionService/Bucktuality.SessionService.csproj Bucktuality.SessionService/
COPY Bucktuality.Shared/Bucktuality.Shared.csproj Bucktuality.Shared/

RUN dotnet restore Bucktuality.SessionService/Bucktuality.SessionService.csproj

COPY . .

RUN dotnet publish Bucktuality.SessionService/Bucktuality.SessionService.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 5011

ENV ASPNETCORE_URLS=http://+:5011

ENTRYPOINT ["dotnet", "Bucktuality.SessionService.dll"]
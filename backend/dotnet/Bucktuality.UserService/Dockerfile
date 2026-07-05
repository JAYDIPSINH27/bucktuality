FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Bucktuality.UserService/Bucktuality.UserService.csproj Bucktuality.UserService/
COPY Bucktuality.Shared/Bucktuality.Shared.csproj Bucktuality.Shared/

RUN dotnet restore Bucktuality.UserService/Bucktuality.UserService.csproj

COPY . .

RUN dotnet publish Bucktuality.UserService/Bucktuality.UserService.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 5010

ENV ASPNETCORE_URLS=http://+:5010

ENTRYPOINT ["dotnet", "Bucktuality.UserService.dll"]
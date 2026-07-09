FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Bucktuality.AdminService/Bucktuality.AdminService.csproj Bucktuality.AdminService/
COPY Bucktuality.Shared/Bucktuality.Shared.csproj Bucktuality.Shared/

RUN dotnet restore Bucktuality.AdminService/Bucktuality.AdminService.csproj

COPY . .

RUN dotnet publish Bucktuality.AdminService/Bucktuality.AdminService.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 5013

ENV ASPNETCORE_URLS=http://+:5013

ENTRYPOINT ["dotnet", "Bucktuality.AdminService.dll"]
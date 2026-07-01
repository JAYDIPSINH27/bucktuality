using Bucktuality.RealtimeService.Hubs;
using Bucktuality.RealtimeService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();

builder.Services.AddHttpClient<MatchmakingClient>(client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:Matchmaking"] ?? "http://localhost:8081"
    );
});

builder.Services.AddHttpClient<SessionClient>(client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:Session"] ?? "http://localhost:5011"
    );
});

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials()
            .SetIsOriginAllowed(_ => true);
    });
});

var app = builder.Build();

app.UseCors();

app.MapGet("/health", () => new
{
    service = "realtime-service",
    status = "healthy"
});

app.MapHub<ChatHub>("/chatHub");

app.Run();
using System.Net.Http.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .AllowAnyHeader()
            .AllowAnyMethod()
            .SetIsOriginAllowed(_ => true);
    });
});

builder.Services.AddHttpClient("analytics", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:Analytics"] ?? "http://analytics-service:8083"
    );
});

builder.Services.AddHttpClient("presence", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:Presence"] ?? "http://presence-service:8084"
    );
});

builder.Services.AddHttpClient("notifications", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:Notifications"] ?? "http://notification-service:8085"
    );
});

builder.Services.AddHttpClient("moderation", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:Moderation"] ?? "http://moderation-service:5012"
    );
});

builder.Services.AddHttpClient("ai", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Services:AiModeration"] ?? "http://ai-moderation-service:8090"
    );
});

var app = builder.Build();

app.UseCors();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => new
{
    service = "admin-service",
    status = "healthy"
});

app.MapGet("/admin/summary", async (IHttpClientFactory httpClientFactory) =>
{
    var analyticsClient = httpClientFactory.CreateClient("analytics");
    var presenceClient = httpClientFactory.CreateClient("presence");
    var notificationsClient = httpClientFactory.CreateClient("notifications");
    var moderationClient = httpClientFactory.CreateClient("moderation");
    var aiClient = httpClientFactory.CreateClient("ai");

    var analytics = await SafeGetAsync<object>(
        analyticsClient,
        "/analytics/summary"
    );

    var presence = await SafeGetAsync<object>(
        presenceClient,
        "/presence/summary"
    );

    var notifications = await SafeGetAsync<object>(
        notificationsClient,
        "/notifications"
    );

    var reports = await SafeGetAsync<object>(
        moderationClient,
        "/reports"
    );

    var flaggedMessages = await SafeGetAsync<object>(
        aiClient,
        "/moderation/flagged"
    );

    return Results.Ok(new
    {
        generatedAtUtc = DateTime.UtcNow,
        analytics,
        presence,
        notifications,
        reports,
        flaggedMessages
    });
});

app.Run();

static async Task<object?> SafeGetAsync<T>(
    HttpClient client,
    string path)
{
    try
    {
        var response = await client.GetAsync(path);

        if (!response.IsSuccessStatusCode)
        {
            return new
            {
                error = true,
                statusCode = response.StatusCode.ToString()
            };
        }

        return await response.Content.ReadFromJsonAsync<T>();
    }
    catch (Exception ex)
    {
        return new
        {
            error = true,
            message = ex.Message
        };
    }
}
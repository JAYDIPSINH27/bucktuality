using Bucktuality.SessionService.Data;
using Bucktuality.SessionService.Models;
using Bucktuality.Shared.Contracts;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<SessionDbContext>(options =>
{
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("DefaultConnection"));
});

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

var app = builder.Build();

/*
 * Local-lab migration strategy:
 * Retry until SQL Server is reachable, then apply pending migrations.
 *
 * Later, for production, we will replace this with a Kubernetes
 * migration Job so only one controlled process changes the schema.
 */
await ApplyMigrationsWithRetryAsync(app);

app.UseCors();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => new
{
    service = "session-service",
    status = "healthy"
});

app.MapPost("/sessions", async (
    CreateSessionRequest request,
    SessionDbContext db) =>
{
    if (string.IsNullOrWhiteSpace(request.RoomId))
    {
        return Results.BadRequest(new
        {
            message = "RoomId is required."
        });
    }

    if (string.IsNullOrWhiteSpace(request.User1Id) ||
        string.IsNullOrWhiteSpace(request.User2Id))
    {
        return Results.BadRequest(new
        {
            message = "Both user IDs are required."
        });
    }

    var existing = await db.ChatSessions
        .FirstOrDefaultAsync(session =>
            session.RoomId == request.RoomId &&
            session.Status == "active");

    if (existing is not null)
    {
        return Results.Ok(existing);
    }

    var session = new ChatSession
    {
        Id = Guid.NewGuid(),
        RoomId = request.RoomId,
        User1Id = request.User1Id,
        User2Id = request.User2Id,
        StartedAtUtc = DateTime.UtcNow,
        Status = "active"
    };

    db.ChatSessions.Add(session);
    await db.SaveChangesAsync();

    return Results.Ok(session);
});

app.MapPost("/sessions/end", async (
    EndSessionRequest request,
    SessionDbContext db) =>
{
    if (string.IsNullOrWhiteSpace(request.RoomId))
    {
        return Results.BadRequest(new
        {
            message = "RoomId is required."
        });
    }

    var session = await db.ChatSessions
        .FirstOrDefaultAsync(item =>
            item.RoomId == request.RoomId &&
            item.Status == "active");

    if (session is null)
    {
        return Results.NotFound(new
        {
            message = "Active session was not found."
        });
    }

    session.Status = "ended";
    session.EndedAtUtc = DateTime.UtcNow;

    await db.SaveChangesAsync();

    return Results.Ok(session);
});

app.MapGet("/sessions/room/{roomId}", async (
    string roomId,
    SessionDbContext db) =>
{
    var session = await db.ChatSessions
        .OrderByDescending(item => item.StartedAtUtc)
        .FirstOrDefaultAsync(item => item.RoomId == roomId);

    return session is null
        ? Results.NotFound()
        : Results.Ok(session);
});

app.Run();

static async Task ApplyMigrationsWithRetryAsync(
    WebApplication app,
    int maximumAttempts = 12)
{
    for (var attempt = 1; attempt <= maximumAttempts; attempt++)
    {
        try
        {
            await using var scope = app.Services.CreateAsyncScope();

            var db = scope.ServiceProvider
                .GetRequiredService<SessionDbContext>();

            app.Logger.LogInformation(
                "Applying Session Service migrations. Attempt {Attempt}/{MaximumAttempts}",
                attempt,
                maximumAttempts);

            await db.Database.MigrateAsync();

            app.Logger.LogInformation(
                "Session Service database migrations completed.");

            return;
        }
        catch (Exception exception) when (attempt < maximumAttempts)
        {
            app.Logger.LogWarning(
                exception,
                "Session database is unavailable. Retrying in 5 seconds.");

            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }

    throw new InvalidOperationException(
        "Session Service could not apply database migrations.");
}
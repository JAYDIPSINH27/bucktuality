using Bucktuality.SessionService.Data;
using Bucktuality.SessionService.Models;
using Bucktuality.Shared.Contracts;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<SessionDbContext>(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

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
    var existing = await db.ChatSessions
        .FirstOrDefaultAsync(x => x.RoomId == request.RoomId && x.Status == "active");

    if (existing != null)
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
    var session = await db.ChatSessions
        .FirstOrDefaultAsync(x => x.RoomId == request.RoomId && x.Status == "active");

    if (session == null)
    {
        return Results.NotFound();
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
        .FirstOrDefaultAsync(x => x.RoomId == roomId);

    return session == null ? Results.NotFound() : Results.Ok(session);
});

app.Run();
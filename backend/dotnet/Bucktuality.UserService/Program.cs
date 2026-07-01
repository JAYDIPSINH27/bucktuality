using Bucktuality.Shared.Contracts;
using Bucktuality.UserService.Data;
using Bucktuality.UserService.Models;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<UserDbContext>(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
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

app.UseCors();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => new
{
    service = "user-service",
    status = "healthy"
});

app.MapPost("/users/anonymous", async (
    CreateUserRequest request,
    UserDbContext db) =>
{
    var user = new User
    {
        Id = Guid.NewGuid(),
        DisplayName = string.IsNullOrWhiteSpace(request.DisplayName)
            ? $"Guest-{Random.Shared.Next(1000, 9999)}"
            : request.DisplayName,
        Vibe = request.Vibe,
        IsAnonymous = true,
        CreatedAtUtc = DateTime.UtcNow
    };

    db.Users.Add(user);
    await db.SaveChangesAsync();

    return Results.Ok(new UserResponse
    {
        UserId = user.Id,
        DisplayName = user.DisplayName,
        Vibe = user.Vibe,
        CreatedAtUtc = user.CreatedAtUtc
    });
});

app.MapGet("/users/{id:guid}", async (Guid id, UserDbContext db) =>
{
    var user = await db.Users.FindAsync(id);

    if (user == null)
    {
        return Results.NotFound();
    }

    return Results.Ok(new UserResponse
    {
        UserId = user.Id,
        DisplayName = user.DisplayName,
        Vibe = user.Vibe,
        CreatedAtUtc = user.CreatedAtUtc
    });
});

app.Run();
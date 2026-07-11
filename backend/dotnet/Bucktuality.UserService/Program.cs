using Bucktuality.Shared.Contracts;
using Bucktuality.UserService.Data;
using Bucktuality.UserService.Models;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<UserDbContext>(options =>
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
 * Apply pending EF Core migrations.
 *
 * SQL Server may still be starting when the pod launches,
 * so we retry instead of immediately crashing the container.
 */
await ApplyMigrationsWithRetryAsync(app);

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

app.MapGet("/users/{id:guid}", async (
    Guid id,
    UserDbContext db) =>
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
                .GetRequiredService<UserDbContext>();

            app.Logger.LogInformation(
                "Applying User Service migrations. Attempt {Attempt}/{MaximumAttempts}",
                attempt,
                maximumAttempts);

            await db.Database.MigrateAsync();

            app.Logger.LogInformation(
                "User Service database migrations completed.");

            return;
        }
        catch (Exception exception) when (attempt < maximumAttempts)
        {
            app.Logger.LogWarning(
                exception,
                "Database is unavailable. Retrying migration in 5 seconds.");

            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }

    throw new InvalidOperationException(
        "User Service could not apply database migrations.");
}
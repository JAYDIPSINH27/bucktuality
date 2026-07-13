using Bucktuality.ModerationService.Data;
using Bucktuality.ModerationService.Models;
using Bucktuality.ModerationService.Services;
using Bucktuality.Shared.Contracts;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ModerationDbContext>(options =>
{
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("DefaultConnection"));
});

builder.Services.AddSingleton<KafkaProducerService>();

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

await ApplyMigrationsWithRetryAsync(app);

app.UseCors();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => new
{
    service = "moderation-service",
    status = "healthy"
});

app.MapPost("/reports", async (
    CreateReportRequest request,
    ModerationDbContext db,
    KafkaProducerService kafkaProducer) =>
{
    if (string.IsNullOrWhiteSpace(request.RoomId))
    {
        return Results.BadRequest(new
        {
            message = "RoomId is required."
        });
    }

    if (string.IsNullOrWhiteSpace(request.ReporterUserId))
    {
        return Results.BadRequest(new
        {
            message = "ReporterUserId is required."
        });
    }

    if (string.IsNullOrWhiteSpace(request.ReportedUserId))
    {
        return Results.BadRequest(new
        {
            message = "ReportedUserId is required."
        });
    }

    if (string.IsNullOrWhiteSpace(request.Reason))
    {
        return Results.BadRequest(new
        {
            message = "Reason is required."
        });
    }

    var report = new Report
    {
        Id = Guid.NewGuid(),
        RoomId = request.RoomId,
        ReporterUserId = request.ReporterUserId,
        ReportedUserId = request.ReportedUserId,
        Reason = request.Reason,
        CreatedAtUtc = DateTime.UtcNow
    };

    db.Reports.Add(report);
    await db.SaveChangesAsync();

    /*
     * This publish attempt may fail until Kafka is deployed in Kubernetes.
     * KafkaProducerService already catches and logs the failure, so the
     * report remains saved successfully in SQL Server.
     */
    await kafkaProducer.PublishAsync("user-reported", new UserReportedEvent
    {
        ReportId = report.Id.ToString(),
        RoomId = report.RoomId,
        ReporterUserId = report.ReporterUserId,
        ReportedUserId = report.ReportedUserId,
        Reason = report.Reason,
        CreatedAtUtc = report.CreatedAtUtc
    });

    return Results.Ok(new ReportResponse
    {
        ReportId = report.Id,
        RoomId = report.RoomId,
        ReporterUserId = report.ReporterUserId,
        ReportedUserId = report.ReportedUserId,
        Reason = report.Reason,
        CreatedAtUtc = report.CreatedAtUtc
    });
});

app.MapGet("/reports", async (ModerationDbContext db) =>
{
    var reports = await db.Reports
        .OrderByDescending(report => report.CreatedAtUtc)
        .Take(100)
        .ToListAsync();

    return Results.Ok(reports);
});

app.MapGet("/reports/user/{userId}", async (
    string userId,
    ModerationDbContext db) =>
{
    var reports = await db.Reports
        .Where(report => report.ReportedUserId == userId)
        .OrderByDescending(report => report.CreatedAtUtc)
        .ToListAsync();

    return Results.Ok(reports);
});

app.Run();

static async Task ApplyMigrationsWithRetryAsync(
    WebApplication app,
    int maximumAttempts = 12)
{
    Exception? lastException = null;

    for (var attempt = 1; attempt <= maximumAttempts; attempt++)
    {
        try
        {
            await using var scope = app.Services.CreateAsyncScope();

            var db = scope.ServiceProvider
                .GetRequiredService<ModerationDbContext>();

            app.Logger.LogInformation(
                "Applying Moderation Service migrations. Attempt {Attempt}/{MaximumAttempts}",
                attempt,
                maximumAttempts);

            await db.Database.MigrateAsync();

            app.Logger.LogInformation(
                "Moderation Service database migrations completed.");

            return;
        }
        catch (Exception exception)
        {
            lastException = exception;

            if (attempt == maximumAttempts)
            {
                break;
            }

            app.Logger.LogWarning(
                exception,
                "Moderation database unavailable. Retrying in 5 seconds.");

            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }

    throw new InvalidOperationException(
        "Moderation Service could not apply database migrations.",
        lastException);
}
using Bucktuality.ModerationService.Data;
using Bucktuality.ModerationService.Models;
using Bucktuality.ModerationService.Services;
using Bucktuality.Shared.Contracts;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ModerationDbContext>(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
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
        return Results.BadRequest("RoomId is required.");
    }

    if (string.IsNullOrWhiteSpace(request.ReporterUserId))
    {
        return Results.BadRequest("ReporterUserId is required.");
    }

    if (string.IsNullOrWhiteSpace(request.ReportedUserId))
    {
        return Results.BadRequest("ReportedUserId is required.");
    }

    if (string.IsNullOrWhiteSpace(request.Reason))
    {
        return Results.BadRequest("Reason is required.");
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
        .OrderByDescending(x => x.CreatedAtUtc)
        .Take(100)
        .ToListAsync();

    return Results.Ok(reports);
});

app.MapGet("/reports/user/{userId}", async (
    string userId,
    ModerationDbContext db) =>
{
    var reports = await db.Reports
        .Where(x => x.ReportedUserId == userId)
        .OrderByDescending(x => x.CreatedAtUtc)
        .ToListAsync();

    return Results.Ok(reports);
});

app.Run();
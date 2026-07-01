namespace Bucktuality.ModerationService.Models;

public class Report
{
    public Guid Id { get; set; }

    public string RoomId { get; set; } = string.Empty;

    public string ReporterUserId { get; set; } = string.Empty;

    public string ReportedUserId { get; set; } = string.Empty;

    public string Reason { get; set; } = string.Empty;

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}
namespace Bucktuality.Shared.Contracts;

public class UserReportedEvent
{
    public string EventType { get; set; } = "UserReported";
    public string ReportId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string ReporterUserId { get; set; } = string.Empty;
    public string ReportedUserId { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}
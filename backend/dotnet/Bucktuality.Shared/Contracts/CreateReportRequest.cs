namespace Bucktuality.Shared.Contracts;

public class CreateReportRequest
{
    public string RoomId { get; set; } = string.Empty;
    public string ReporterUserId { get; set; } = string.Empty;
    public string ReportedUserId { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}
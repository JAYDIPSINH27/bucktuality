namespace Bucktuality.SessionService.Models;

public class ChatSession
{
    public Guid Id { get; set; }
    public string RoomId { get; set; } = string.Empty;
    public string User1Id { get; set; } = string.Empty;
    public string User2Id { get; set; } = string.Empty;
    public DateTime StartedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? EndedAtUtc { get; set; }
    public string Status { get; set; } = "active";
}
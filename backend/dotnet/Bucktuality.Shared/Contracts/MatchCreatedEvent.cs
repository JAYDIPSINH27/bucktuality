namespace Bucktuality.Shared.Contracts;

public class MatchCreatedEvent
{
    public string EventType { get; set; } = "MatchCreated";
    public string RoomId { get; set; } = string.Empty;
    public string User1Id { get; set; } = string.Empty;
    public string User2Id { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}
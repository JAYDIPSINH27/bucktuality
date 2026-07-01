namespace Bucktuality.Shared.Contracts;

public class MessageSentEvent
{
    public string EventType { get; set; } = "MessageSent";
    public string RoomId { get; set; } = string.Empty;
    public string SenderUserId { get; set; } = string.Empty;
    public DateTime SentAtUtc { get; set; } = DateTime.UtcNow;
}
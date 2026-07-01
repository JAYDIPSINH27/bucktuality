namespace Bucktuality.Shared.Contracts;

public class ChatMessage
{
    public string RoomId { get; set; } = string.Empty;
    public string SenderUserId { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public DateTime SentAt { get; set; } = DateTime.UtcNow;
}
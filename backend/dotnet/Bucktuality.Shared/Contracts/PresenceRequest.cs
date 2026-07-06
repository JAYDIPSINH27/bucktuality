namespace Bucktuality.Shared.Contracts;

public class PresenceRequest
{
    public string UserId { get; set; } = string.Empty;
    public string ConnectionId { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string? RoomId { get; set; }
    public bool CameraOn { get; set; }
    public bool MicOn { get; set; }
}
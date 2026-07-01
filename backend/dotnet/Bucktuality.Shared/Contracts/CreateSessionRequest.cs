namespace Bucktuality.Shared.Contracts;

public class CreateSessionRequest
{
    public string RoomId { get; set; } = string.Empty;
    public string User1Id { get; set; } = string.Empty;
    public string User2Id { get; set; } = string.Empty;
}
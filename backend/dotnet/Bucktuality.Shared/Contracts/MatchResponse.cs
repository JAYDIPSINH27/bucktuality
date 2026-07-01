namespace Bucktuality.Shared.Contracts;

public class MatchResponse
{
    public bool IsMatched { get; set; }
    public string Status { get; set; } = "waiting";
    public string? RoomId { get; set; }
    public string? PartnerUserId { get; set; }
    public string? PartnerConnectionId { get; set; }
}